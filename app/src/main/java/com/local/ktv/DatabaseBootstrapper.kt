package com.local.ktv

import org.json.JSONObject
import java.io.BufferedOutputStream
import java.io.File
import java.io.FileOutputStream
import java.net.HttpURLConnection
import java.net.URL
import java.security.MessageDigest
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/** Downloads catalog chunks concurrently, then verifies and installs the merged database atomically. */
object DatabaseBootstrapper {
    private const val PARALLEL_DOWNLOADS = 4
    val manifestUrl: String get() = BuildConfig.GITEE_DATABASE_MANIFEST_URL

    fun download(onProgress: (Int) -> Unit): Result<File> = runCatching {
        val sourceManifestUrl = manifestUrl
        require(sourceManifestUrl.isNotBlank()) { "Gitee database URL is not configured" }
        val manifest = JSONObject(readText(sourceManifestUrl))
        val chunksJson = manifest.optJSONArray("chunks")
            ?: throw IllegalArgumentException("database manifest has no chunks")
        require(chunksJson.length() > 0) { "database manifest has no chunks" }

        // Some published manifests omit the aggregate "size" field but include
        // each chunk's byte size. Derive the total instead of failing with
        // JSONObject's opaque "No value for size" exception.
        val declaredChunkSizes = (0 until chunksJson.length()).map { index ->
            chunksJson.getJSONObject(index).optLong("size", -1L)
        }
        val expectedSize = manifest.optLong("size", -1L).takeIf { it > 0L }
            ?: declaredChunkSizes.takeIf { sizes -> sizes.all { it >= 0L } }?.sum()
            ?: throw IllegalArgumentException(
                "database manifest is missing size and valid per-chunk sizes"
            )
        val expectedSha256 = manifest.optString("sha256", "").trim()
        require(expectedSha256.matches(Regex("(?i)[0-9a-f]{64}"))) {
            "database manifest is missing a valid SHA-256 checksum"
        }

        val target = MuseDatabase.defaultDbFile()
        val temporary = File(target.parentFile, "${target.name}.download")
        val partsDir = File(target.parentFile, "${target.name}.parts")
        target.parentFile?.mkdirs()
        temporary.delete()
        partsDir.deleteRecursively()
        check(partsDir.mkdirs()) { "cannot create database parts directory" }

        val urls = List(chunksJson.length()) { index ->
            val chunk = chunksJson.getJSONObject(index)
            val chunkUrl = chunk.optString("url", "").trim()
            require(chunkUrl.isNotEmpty()) { "database manifest chunk $index has no URL" }
            resolveUrl(sourceManifestUrl, chunkUrl)
        }
        val written = AtomicLong(0L)
        val lastProgress = AtomicInteger(-1)
        val executor = Executors.newFixedThreadPool(PARALLEL_DOWNLOADS.coerceAtMost(urls.size))
        try {
            val futures = urls.mapIndexed { index, url ->
                executor.submit<File> {
                    val part = File(partsDir, "part-${index.toString().padStart(5, '0')}")
                    downloadPart(url, part) { count ->
                        val progress = (written.addAndGet(count.toLong()) * 100L /
                            expectedSize.coerceAtLeast(1L)).toInt().coerceIn(0, 99)
                        while (true) {
                            val previous = lastProgress.get()
                            if (progress <= previous || lastProgress.compareAndSet(previous, progress)) break
                        }
                        if (lastProgress.get() == progress) onProgress(progress)
                    }
                    part
                }
            }
            val parts = futures.map { it.get() }
            BufferedOutputStream(FileOutputStream(temporary)).use { output ->
                parts.forEach { part -> part.inputStream().buffered(256 * 1024).use { it.copyTo(output, 256 * 1024) } }
            }
        } finally {
            executor.shutdownNow()
            executor.awaitTermination(5, TimeUnit.SECONDS)
        }

        check(temporary.length() == expectedSize) {
            "database size mismatch: ${temporary.length()} != $expectedSize"
        }
        check(sha256(temporary).equals(expectedSha256, ignoreCase = true)) {
            "database checksum mismatch"
        }
        if (target.exists()) check(target.delete()) { "cannot replace old database" }
        check(temporary.renameTo(target)) { "cannot install downloaded database" }
        partsDir.deleteRecursively()
        onProgress(100)
        target
    }.onFailure {
        val target = MuseDatabase.defaultDbFile()
        File(target.parentFile, "${target.name}.download").delete()
        File(target.parentFile, "${target.name}.parts").deleteRecursively()
    }

    private fun downloadPart(url: String, target: File, onBytes: (Int) -> Unit) {
        val connection = connection(url)
        try {
            connection.inputStream.buffered(256 * 1024).use { input ->
                target.outputStream().buffered(256 * 1024).use { output ->
                    val buffer = ByteArray(256 * 1024)
                    while (true) {
                        val count = input.read(buffer)
                        if (count < 0) break
                        output.write(buffer, 0, count)
                        onBytes(count)
                    }
                }
            }
        } finally {
            connection.disconnect()
        }
    }

    private fun readText(url: String): String {
        val connection = connection(url)
        return try {
            connection.inputStream.bufferedReader(Charsets.UTF_8).use { it.readText() }
        } finally {
            connection.disconnect()
        }
    }

    private fun connection(url: String): HttpURLConnection =
        (URL(url).openConnection() as HttpURLConnection).apply {
            connectTimeout = 15_000
            readTimeout = 60_000
            instanceFollowRedirects = true
            setRequestProperty("User-Agent", "MaidongKTV/1.1")
            val code = responseCode
            check(code in 200..299) { "HTTP $code: $url" }
        }

    private fun resolveUrl(manifestUrl: String, value: String): String =
        if (value.startsWith("http://") || value.startsWith("https://")) value
        else URL(URL(manifestUrl), value).toString()

    private fun sha256(file: File): String {
        val digest = MessageDigest.getInstance("SHA-256")
        file.inputStream().buffered(256 * 1024).use { input ->
            val buffer = ByteArray(256 * 1024)
            while (true) {
                val count = input.read(buffer)
                if (count < 0) break
                digest.update(buffer, 0, count)
            }
        }
        return digest.digest().joinToString("") { "%02x".format(it.toInt() and 0xff) }
    }
}
