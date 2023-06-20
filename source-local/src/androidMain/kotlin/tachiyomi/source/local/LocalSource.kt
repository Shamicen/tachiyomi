package tachiyomi.source.local

import android.content.Context
import eu.kanade.tachiyomi.source.CatalogueSource
import eu.kanade.tachiyomi.source.Source
import eu.kanade.tachiyomi.source.UnmeteredSource
import eu.kanade.tachiyomi.source.model.Filter
import eu.kanade.tachiyomi.source.model.FilterList
import eu.kanade.tachiyomi.source.model.MangasPage
import eu.kanade.tachiyomi.source.model.SChapter
import eu.kanade.tachiyomi.source.model.SManga
import eu.kanade.tachiyomi.util.lang.compareToCaseInsensitiveNaturalOrder
import eu.kanade.tachiyomi.util.storage.EpubFile
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.decodeFromStream
import logcat.LogPriority
import nl.adaptivity.xmlutil.AndroidXmlReader
import nl.adaptivity.xmlutil.serialization.XML
import rx.Observable
import tachiyomi.core.metadata.comicinfo.COMIC_INFO_FILE
import tachiyomi.core.metadata.comicinfo.ComicInfo
import tachiyomi.core.metadata.comicinfo.copyFromComicInfo
import tachiyomi.core.metadata.tachiyomi.MangaDetails
import tachiyomi.core.util.lang.withIOContext
import tachiyomi.core.util.system.ImageUtil
import tachiyomi.core.util.system.logcat
import tachiyomi.domain.chapter.service.ChapterRecognition
import tachiyomi.domain.manga.model.Manga
import tachiyomi.domain.manga.repository.MangaRepository
import tachiyomi.source.local.filter.OrderBy
import tachiyomi.source.local.image.LocalCoverManager
import tachiyomi.source.local.io.Archive
import tachiyomi.source.local.io.Format
import tachiyomi.source.local.io.LocalSourceFileSystem
import tachiyomi.source.local.metadata.fillChapterMetadata
import tachiyomi.source.local.metadata.fillMangaMetadata
import uy.kohesive.injekt.injectLazy
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import java.nio.charset.StandardCharsets
import java.util.zip.ZipFile
import kotlin.streams.toList
import kotlin.time.Duration.Companion.days
import com.github.junrar.Archive as JunrarArchive
import tachiyomi.domain.source.model.Source as DomainSource

actual class LocalSource(
    private val context: Context,
    private val fileSystem: LocalSourceFileSystem,
    private val coverManager: LocalCoverManager,
) : CatalogueSource, UnmeteredSource {

    private val json: Json by injectLazy()
    private val xml: XML by injectLazy()
    private val mangaRepository: MangaRepository by injectLazy()

    private var localManga: List<SManga> = emptyList()

    private val mangaChunks = fileSystem.getFilesInBaseDirectories()
        // Filter out files that are hidden and is not a folder
        .filter { it.isDirectory && !it.name.startsWith('.') }
        .distinctBy { it.name }
        .toList()
        .chunked(MANGA_LOADING_CHUNK_SIZE)

    private var allMangaLoaded = false

    private var includedChunkIndex = -1

    private val POPULAR_FILTERS = FilterList(OrderBy.Popular(context))
    private val LATEST_FILTERS = FilterList(OrderBy.Latest(context))

    override val name: String = context.getString(R.string.local_source)

    override val id: Long = ID

    override val lang: String = "other"

    override fun toString() = name

    override val supportsLatest: Boolean = true

    private fun loadMangaForPage(page: Int) {
        if (localManga.size >= page * MANGA_LOADING_CHUNK_SIZE) return
        // don't load last page multiple times
        if (localManga.size.mod(MANGA_LOADING_CHUNK_SIZE) != 0) return

        localManga = localManga.plus(
            mangaChunks[page - 1].parallelStream().map { mangaDir ->
                SManga.create().apply manga@{
                    url = mangaDir.name
                    lastModified = mangaDir.lastModified()

                    val localMangaList = runBlocking { getMangaList() }
                    title = localMangaList[url]?.title ?: mangaDir.name
                    author = localMangaList[url]?.author
                    artist = localMangaList[url]?.artist
                    description = localMangaList[url]?.description
                    genre = localMangaList[url]?.genre?.joinToString(", ") { it.trim() }
                    status = localMangaList[url]?.status?.toInt() ?: getStatusIntFromString("Unknown")

                    // Try to find the cover
                    coverManager.find(mangaDir.name)
                        ?.takeIf(File::exists)
                        ?.let { thumbnail_url = it.absolutePath }

                    // Fetch chapters and fill metadata
                    runBlocking {
                        val chapters = getChapterList(this@manga)
                        if (chapters.isNotEmpty()) {
                            val chapter = chapters.last()

                            // only read metadata from disk if no optional field has metadata in the database yet
                            if (
                                author.isNullOrBlank() &&
                                artist.isNullOrBlank() &&
                                description.isNullOrBlank() &&
                                genre.isNullOrBlank() &&
                                status == getStatusIntFromString("Unknown")
                            ) {
                                when (val format = getFormat(chapter)) {
                                    is Format.Directory -> getMangaDetails(this@manga)
                                    is Format.Zip -> getMangaDetails(this@manga)
                                    is Format.Rar -> getMangaDetails(this@manga)
                                    is Format.Epub -> EpubFile(format.file).use { epub ->
                                        epub.fillMangaMetadata(this@manga)
                                    }
                                }
                            }

                            // Copy the cover from the first chapter found if not available
                            if (this@manga.thumbnail_url == null) {
                                updateCover(chapter, this@manga)
                            }
                        }
                    }
                }
            }.toList(),
        )
    }

    // Browse related
    override fun fetchPopularManga(page: Int) = fetchSearchManga(page, "", POPULAR_FILTERS)

    override fun fetchLatestUpdates(page: Int) = fetchSearchManga(page, "", LATEST_FILTERS)
    enum class OrderByPopular {
        NOT_SET,
        POPULAR_ASCENDING,
        POPULAR_DESCENDING,
    }
    enum class OrderByLatest {
        NOT_SET,
        LATEST,
        OLDEST,
    }
    override fun fetchSearchManga(page: Int, query: String, filters: FilterList): Observable<MangasPage> {
        loadMangaForPage(page)

        var includedManga: MutableList<SManga>

        val lastModifiedLimit by lazy { if (filters === LATEST_FILTERS) System.currentTimeMillis() - LATEST_THRESHOLD else 0L }
        val searchManga = localManga
            .filter { manga -> // Filter by query or last modified
                if (lastModifiedLimit == 0L) {
                    manga.title.contains(query, ignoreCase = true) ||
                        File(manga.url).name.contains(query, ignoreCase = true)
                } else {
                    File(manga.url).lastModified() >= lastModifiedLimit
                }
            }

        var orderByPopular = OrderByPopular.NOT_SET
        var orderByLatest = OrderByLatest.NOT_SET

        val includedGenres = mutableListOf<String>()
        val includedAuthors = mutableListOf<String>()
        val includedArtists = mutableListOf<String>()
        val includedStatuses = mutableListOf<String>()

        filters.forEach { filter ->
            when (filter) {
                is OrderBy.Popular -> {
                    orderByPopular = if (filter.state!!.ascending) {
                        OrderByPopular.POPULAR_ASCENDING
                    } else {
                        OrderByPopular.POPULAR_DESCENDING
                    }
                }

                is OrderBy.Latest -> {
                    orderByLatest = if (filter.state!!.ascending) {
                        OrderByLatest.LATEST
                    } else {
                        OrderByLatest.OLDEST
                    }
                }

                // included Filter
                is GenreGroup -> {
                    filter.state.forEach { genre ->
                        when (genre.state) {
                            Filter.TriState.STATE_INCLUDE -> {
                                includedGenres.add(genre.name)
                            }
                        }
                    }
                }
                is AuthorGroup -> {
                    filter.state.forEach { author ->
                        when (author.state) {
                            Filter.TriState.STATE_INCLUDE -> {
                                includedAuthors.add(author.name)
                            }
                        }
                    }
                }
                is ArtistGroup -> {
                    filter.state.forEach { artist ->
                        when (artist.state) {
                            Filter.TriState.STATE_INCLUDE -> {
                                includedArtists.add(artist.name)
                            }
                        }
                    }
                }
                is StatusGroup -> {
                    filter.state.forEach { status ->
                        when (status.state) {
                            Filter.TriState.STATE_INCLUDE -> {
                                includedStatuses.add(status.name)
                            }
                        }
                    }
                }

                else -> {
                    /* Do nothing */
                }
            }
        }

        includedManga = searchManga.filter { manga ->
            areAllElementsInMangaEntry(includedGenres, manga.genre) &&
                areAllElementsInMangaEntry(includedAuthors, manga.author) &&
                areAllElementsInMangaEntry(includedArtists, manga.artist) &&
                (if (includedStatuses.isNotEmpty()) includedStatuses.map { getStatusIntFromString(it) }.contains(manga.status) else true)
        }.toMutableList()

        if (includedGenres.isEmpty() &&
            includedAuthors.isEmpty() &&
            includedArtists.isEmpty() &&
            includedStatuses.isEmpty()
        ) {
            includedManga = searchManga.toMutableList()
        }

        when (orderByPopular) {
            OrderByPopular.POPULAR_ASCENDING ->
                includedManga = if (allMangaLoaded) {
                    includedManga.sortedWith(compareBy(String.CASE_INSENSITIVE_ORDER) { it.title })
                        .toMutableList()
                } else {
                    includedManga
                }

            OrderByPopular.POPULAR_DESCENDING ->
                includedManga = if (allMangaLoaded) {
                    includedManga.sortedWith(compareByDescending(String.CASE_INSENSITIVE_ORDER) { it.title })
                        .toMutableList()
                } else {
                    includedManga
                }

            OrderByPopular.NOT_SET -> Unit
        }

        when (orderByLatest) {
            OrderByLatest.LATEST ->
                includedManga = if (allMangaLoaded) {
                    includedManga.sortedBy { it.lastModified }
                        .toMutableList()
                } else {
                    includedManga
                }

            OrderByLatest.OLDEST ->
                includedManga = if (allMangaLoaded) {
                    includedManga.sortedByDescending { it.lastModified }
                        .toMutableList()
                } else {
                    includedManga
                }

            OrderByLatest.NOT_SET -> Unit
        }

        filters.forEach { filter ->
            when (filter) {
                // excluded Filter
                is GenreGroup -> {
                    filter.state.forEach { genre ->
                        when (genre.state) {
                            Filter.TriState.STATE_EXCLUDE -> {
                                includedManga.removeIf { manga ->
                                    manga.genre?.split(",")?.map { it.trim() }?.contains(genre.name) ?: false
                                }
                            }
                        }
                    }
                }
                is AuthorGroup -> {
                    filter.state.forEach { author ->
                        when (author.state) {
                            Filter.TriState.STATE_EXCLUDE -> {
                                includedManga.removeIf { manga ->
                                    manga.author?.split(",")?.map { it.trim() }?.contains(author.name) ?: false
                                }
                            }
                        }
                    }
                }
                is ArtistGroup -> {
                    filter.state.forEach { artist ->
                        when (artist.state) {
                            Filter.TriState.STATE_EXCLUDE -> {
                                includedManga.removeIf { manga ->
                                    manga.artist?.split(",")?.map { it.trim() }?.contains(artist.name) ?: false
                                }
                            }
                        }
                    }
                }
                is StatusGroup -> {
                    filter.state.forEach { status ->
                        when (status.state) {
                            Filter.TriState.STATE_EXCLUDE -> {
                                includedManga.removeIf { manga ->
                                    manga.getStatusString() == status.name
                                }
                            }
                        }
                    }
                }

                else -> {
                    /* Do nothing */
                }
            }
        }
        val mangaPageList = includedManga.toList().chunked(MANGA_LOADING_CHUNK_SIZE)

        if (page == 1) includedChunkIndex = -1
        if (includedChunkIndex <= mangaPageList.lastIndex) {
            includedChunkIndex++
        } else {
            includedChunkIndex = mangaPageList.lastIndex
        }

        val lastLocalMangaPageReached = (mangaChunks.lastIndex == page - 1)
        val lastPage = (lastLocalMangaPageReached || mangaPageList[includedChunkIndex].size.mod(MANGA_LOADING_CHUNK_SIZE) != 0)

        if (lastLocalMangaPageReached) allMangaLoaded = true
        return Observable.just(MangasPage(mangaPageList[includedChunkIndex], !lastPage))
    }

    private fun areAllElementsInMangaEntry(includedList: MutableList<String>, mangaEntry: String?): Boolean {
        return if (includedList.isNotEmpty()) {
            mangaEntry?.split(",")?.map { it.trim() }
                ?.let { mangaEntryList -> includedList.all { it in mangaEntryList } } ?: false
        } else {
            true
        }
    }

    private fun getStatusIntFromString(statusString: String): Int {
        return when (statusString) {
            "Unknown" -> 0
            "Ongoing" -> 1
            "Completed" -> 2
            "Licensed" -> 3
            "Publishing finished" -> 4
            "Cancelled" -> 5
            "On hiatus" -> 6
            else -> 0
        }
    }

    private suspend fun getMangaList(): Map<String?, Manga?> {
        return fileSystem.getFilesInBaseDirectories().toList()
            .filter { it.isDirectory && !it.name.startsWith('.') }
            .map { mangaRepository.getMangaByUrlAndSourceId(it.name, ID) }
            .associateBy { it?.url }
    }

    // Manga details related
    override suspend fun getMangaDetails(manga: SManga): SManga = withIOContext {
        coverManager.find(manga.url)?.let {
            manga.thumbnail_url = it.absolutePath
        }

        // Augment manga details based on metadata files
        try {
            val mangaDirFiles = fileSystem.getFilesInMangaDirectory(manga.url).toList()

            val comicInfoFile = mangaDirFiles
                .firstOrNull { it.name == COMIC_INFO_FILE }
            val noXmlFile = mangaDirFiles
                .firstOrNull { it.name == ".noxml" }
            val legacyJsonDetailsFile = mangaDirFiles
                .firstOrNull { it.extension == "json" }

            when {
                // Top level ComicInfo.xml
                comicInfoFile != null -> {
                    noXmlFile?.delete()
                    setMangaDetailsFromComicInfoFile(comicInfoFile.inputStream(), manga)
                }

                // TODO: automatically convert these to ComicInfo.xml
                legacyJsonDetailsFile != null -> {
                    json.decodeFromStream<MangaDetails>(legacyJsonDetailsFile.inputStream()).run {
                        title?.let { manga.title = it }
                        author?.let { manga.author = it }
                        artist?.let { manga.artist = it }
                        description?.let { manga.description = it }
                        genre?.let { manga.genre = it.joinToString() }
                        status?.let { manga.status = it }
                    }
                }

                // Copy ComicInfo.xml from chapter archive to top level if found
                noXmlFile == null -> {
                    val chapterArchives = mangaDirFiles
                        .filter(Archive::isSupported)
                        .toList()

                    val mangaDir = fileSystem.getMangaDirectory(manga.url)
                    val folderPath = mangaDir?.absolutePath

                    val copiedFile = copyComicInfoFileFromArchive(chapterArchives, folderPath)
                    if (copiedFile != null) {
                        setMangaDetailsFromComicInfoFile(copiedFile.inputStream(), manga)
                    } else {
                        // Avoid re-scanning
                        File("$folderPath/.noxml").createNewFile()
                    }
                }
            }
        } catch (e: Throwable) {
            logcat(LogPriority.ERROR, e) { "Error setting manga details from local metadata for ${manga.title}" }
        }

        return@withIOContext manga
    }

    private fun copyComicInfoFileFromArchive(chapterArchives: List<File>, folderPath: String?): File? {
        for (chapter in chapterArchives) {
            when (Format.valueOf(chapter)) {
                is Format.Zip -> {
                    ZipFile(chapter).use { zip: ZipFile ->
                        zip.getEntry(COMIC_INFO_FILE)?.let { comicInfoFile ->
                            zip.getInputStream(comicInfoFile).buffered().use { stream ->
                                return copyComicInfoFile(stream, folderPath)
                            }
                        }
                    }
                }
                is Format.Rar -> {
                    JunrarArchive(chapter).use { rar ->
                        rar.fileHeaders.firstOrNull { it.fileName == COMIC_INFO_FILE }?.let { comicInfoFile ->
                            rar.getInputStream(comicInfoFile).buffered().use { stream ->
                                return copyComicInfoFile(stream, folderPath)
                            }
                        }
                    }
                }
                else -> {}
            }
        }
        return null
    }

    private fun copyComicInfoFile(comicInfoFileStream: InputStream, folderPath: String?): File {
        return File("$folderPath/$COMIC_INFO_FILE").apply {
            outputStream().use { outputStream ->
                comicInfoFileStream.use { it.copyTo(outputStream) }
            }
        }
    }

    private fun setMangaDetailsFromComicInfoFile(stream: InputStream, manga: SManga) {
        val comicInfo = AndroidXmlReader(stream, StandardCharsets.UTF_8.name()).use {
            xml.decodeFromReader<ComicInfo>(it)
        }

        manga.copyFromComicInfo(comicInfo)
    }

    // Chapters
    override suspend fun getChapterList(manga: SManga): List<SChapter> {
        return fileSystem.getFilesInMangaDirectory(manga.url)
            // Only keep supported formats
            .filter { it.isDirectory || Archive.isSupported(it) }
            .map { chapterFile ->
                SChapter.create().apply {
                    url = "${manga.url}/${chapterFile.name}"
                    name = if (chapterFile.isDirectory) {
                        chapterFile.name
                    } else {
                        chapterFile.nameWithoutExtension
                    }
                    date_upload = chapterFile.lastModified()
                    chapter_number = ChapterRecognition.parseChapterNumber(manga.title, this.name, this.chapter_number)

                    val format = Format.valueOf(chapterFile)
                    if (format is Format.Epub) {
                        EpubFile(format.file).use { epub ->
                            epub.fillChapterMetadata(this)
                        }
                    }
                }
            }
            .sortedWith { c1, c2 ->
                val c = c2.chapter_number.compareTo(c1.chapter_number)
                if (c == 0) c2.name.compareToCaseInsensitiveNaturalOrder(c1.name) else c
            }
            .toList()
    }

    // Filters
    private class GenreFilter(genre: String) : Filter.TriState(genre)
    private class GenreGroup(genres: List<GenreFilter>) : Filter.Group<GenreFilter>("Genres", genres)
    private class AuthorFilter(author: String) : Filter.TriState(author)
    private class AuthorGroup(authors: List<AuthorFilter>) : Filter.Group<AuthorFilter>("Authors", authors)
    private class ArtistFilter(genre: String) : Filter.TriState(genre)
    private class ArtistGroup(genres: List<ArtistFilter>) : Filter.Group<ArtistFilter>("Artists", genres)
    private class StatusFilter(name: String) : Filter.TriState(name)
    private class StatusGroup(filters: List<StatusFilter>) : Filter.Group<StatusFilter>("Status", filters)

    override fun getFilterList(): FilterList {
        val genres = localManga.mapNotNull { it.genre?.split(",") }
            .flatMap { it.map { genre -> genre.trim() } }.toSet()

        val authors = localManga.mapNotNull { it.author?.split(",") }
            .flatMap { it.map { author -> author.trim() } }.toSet()

        val artists = localManga.mapNotNull { it.artist?.split(",") }
            .flatMap { it.map { artist -> artist.trim() } }.toSet()

        val filters = try {
            mutableListOf<Filter<*>>(
                OrderBy.Popular(context),
                GenreGroup(genres.map { GenreFilter(it) }),
                AuthorGroup(authors.map { AuthorFilter(it) }),
                ArtistGroup(artists.map { ArtistFilter(it) }),
                StatusGroup(
                    listOf(
                        "Ongoing",
                        "Completed",
                        "Licensed",
                        "Publishing finished",
                        "Cancelled",
                        "On hiatus",
                        "Unknown",
                    ).map { StatusFilter(it) },
                ),
            )
        } catch (e: Exception) {
            logcat(LogPriority.ERROR, e)
            emptyList()
        }
        return FilterList(filters)
    }

    // Unused stuff
    override suspend fun getPageList(chapter: SChapter) = throw UnsupportedOperationException("Unused")

    fun getFormat(chapter: SChapter): Format {
        try {
            return fileSystem.getBaseDirectories()
                .map { dir -> File(dir, chapter.url) }
                .find { it.exists() }
                ?.let(Format.Companion::valueOf)
                ?: throw Exception(context.getString(R.string.chapter_not_found))
        } catch (e: Format.UnknownFormatException) {
            throw Exception(context.getString(R.string.local_invalid_format))
        } catch (e: Exception) {
            throw e
        }
    }

    private fun updateCover(chapter: SChapter, manga: SManga): File? {
        return try {
            when (val format = getFormat(chapter)) {
                is Format.Directory -> {
                    val entry = format.file.listFiles()
                        ?.sortedWith { f1, f2 -> f1.name.compareToCaseInsensitiveNaturalOrder(f2.name) }
                        ?.find { !it.isDirectory && ImageUtil.isImage(it.name) { FileInputStream(it) } }

                    entry?.let { coverManager.update(manga, it.inputStream()) }
                }
                is Format.Zip -> {
                    ZipFile(format.file).use { zip ->
                        val entry = zip.entries().toList()
                            .sortedWith { f1, f2 -> f1.name.compareToCaseInsensitiveNaturalOrder(f2.name) }
                            .find { !it.isDirectory && ImageUtil.isImage(it.name) { zip.getInputStream(it) } }

                        entry?.let { coverManager.update(manga, zip.getInputStream(it)) }
                    }
                }
                is Format.Rar -> {
                    JunrarArchive(format.file).use { archive ->
                        val entry = archive.fileHeaders
                            .sortedWith { f1, f2 -> f1.fileName.compareToCaseInsensitiveNaturalOrder(f2.fileName) }
                            .find { !it.isDirectory && ImageUtil.isImage(it.fileName) { archive.getInputStream(it) } }

                        entry?.let { coverManager.update(manga, archive.getInputStream(it)) }
                    }
                }
                is Format.Epub -> {
                    EpubFile(format.file).use { epub ->
                        val entry = epub.getImagesFromPages()
                            .firstOrNull()
                            ?.let { epub.getEntry(it) }

                        entry?.let { coverManager.update(manga, epub.getInputStream(it)) }
                    }
                }
            }
        } catch (e: Throwable) {
            logcat(LogPriority.ERROR, e) { "Error updating cover for ${manga.title}" }
            null
        }
    }

    companion object {
        const val ID = 0L
        const val HELP_URL = "https://tachiyomi.org/help/guides/local-manga/"

        private val LATEST_THRESHOLD = 7.days.inWholeMilliseconds
        private const val MANGA_LOADING_CHUNK_SIZE = 20
    }
}

fun Manga.isLocal(): Boolean = source == LocalSource.ID

fun Source.isLocal(): Boolean = id == LocalSource.ID

fun DomainSource.isLocal(): Boolean = id == LocalSource.ID
