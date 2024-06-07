import kotlinx.coroutines.*
import java.io.*
import java.util.*
import kotlin.system.measureTimeMillis

fun main() = runBlocking {
    val pathToFile = "E:\\helpMe\\src\\main\\kotlin\\generated_ips.txt" // Введите путь к вашему файлу
    val runTime = measureTimeMillis { // Подсчет времени выполнения кода
        val uniqNum = uniqIP(pathToFile)
        println("Кол-во уникальных ip: $uniqNum")
    }
    println("Время выполнения: $runTime ms")
}

suspend fun uniqIP(pathToFile: String): Int = withContext(Dispatchers.IO) {
    val tempDir = File("temp")
    if (!tempDir.exists()) tempDir.mkdir() // Создание временной директории

    val chunkSize = 100000 // Размер чанка (можно настраивать)
    val chunkFiles = mutableListOf<File>()
    var chunkValue = 0

    // Считывание данных файла
    BufferedReader(FileReader(pathToFile)).use { reader ->
        var lines = mutableSetOf<String>()
        var line: String?

        // Присваиваем строку, если она не равна 0
        while (reader.readLine().also { line = it } != null) {
            if (!line.isNullOrBlank()) {
                lines.add(line!!)
            }

            // Запись во временный файл
            if (lines.size >= chunkSize) {
                val chunkFile = File(tempDir, "chunk_$chunkValue.txt")
                writeChunkToFile(chunkFile, lines)
                chunkFiles.add(chunkFile)
                lines.clear()
                chunkValue++
            }
        }

        // Если лист не пустой, записываем чанки
        if (lines.isNotEmpty()) {
            val chunkFile = File(tempDir, "chunk_$chunkValue.txt")
            writeChunkToFile(chunkFile, lines)
            chunkFiles.add(chunkFile)
        }
    }

    val uniqCount = сountUniqIPs(chunkFiles)

    tempDir.deleteRecursively() // Удаление временной директории

    uniqCount
}
// Запись данных во временный файл
fun writeChunkToFile(file: File, lines: Set<String>) {
    BufferedWriter(FileWriter(file)).use { writer ->
        val sortedLines = lines.sorted()
        for (line in sortedLines) {
            writer.write(line)
            writer.newLine()
        }
    }
}
// Создание буфферизации для каждого чанка
fun сountUniqIPs(chunkFiles: List<File>): Int {
    val readers = chunkFiles.map { BufferedReader(FileReader(it)) }
    val queue = PriorityQueue<Pair<String, BufferedReader>>(compareBy { it.first })

    try {
        // Создание очереди
        for (reader in readers) {
            val line = reader.readLine()
            if (line != null) {
                queue.add(line to reader)
            }
        }

        var previousIP: String? = null
        var uniqueCount = 0

        // Подсчет уникальных ip
        while (queue.isNotEmpty()) {
            val (ip, reader) = queue.poll()
            if (ip != previousIP) {
                uniqueCount++
                previousIP = ip
            }

            val nextLine = reader.readLine()
            if (nextLine != null) {
                queue.add(nextLine to reader)
            }
        }

        return uniqueCount
    } finally {
        readers.forEach { it.close() } // Освобождение ресурсов
    }
}