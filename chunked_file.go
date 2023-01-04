// The chunked_file library wraps multiple on-disk file chunks, implementing
// the io.ReadSeeker and io.WriteSeeker interfaces backed by multiple
// underlying files.
package chunked_file

import (
	"fmt"
	"io"
	"os"
	"sort"
)

// Satisfies the WriteSeeker interface, in order to allow writing to multiple
// chunked files as if they're a single file. Create this using
// NewChunkedWriter. The caller must call Close() on this when it's no longer
// needed.
type ChunkedWriter struct {
	// If true, then silently overwrite existing files. Otherwise, returns an
	// error if an existing file would be overwritten. Initially set to true by
	// NewChunkedWriter.
	OverwriteFiles bool
	chunkSize      uint64
	baseFilename   string
	files          []*os.File
	// The position at which we're currently writing. If it's at any offset
	// other than 0 in a file, then the files[currentOffset / chunkSize] must
	// have already been created.
	currentOffset int64
	// The maximum offset to which anything has been written.
	maxOffset int64
}

// Closes all underlying chunks, and returns an error if any of the underlying
// files returned an error.
func (w *ChunkedWriter) Close() error {
	var e error
	msg := "Errors occurred"
	for _, f := range w.files {
		e = f.Close()
		if e != nil {
			msg += ", " + e.Error()
		}
	}
	w.files = nil
	w.baseFilename = ""
	w.chunkSize = 0
	if e != nil {
		// TODO: Wrap all of the errors instead
		return fmt.Errorf("%s", msg)
	}
	return nil
}

func (w *ChunkedWriter) getFilename(chunkIndex int64) string {
	if chunkIndex < 0 {
		panic("Invalid chunk index")
	}
	return fmt.Sprintf("%s.part.%d", w.baseFilename, chunkIndex)
}

// Writes chunks of zeros to the given sink, returning an error if one occurs.
// This function exists to ensure that bytes are written in reasonably-sized
// chunks, without needing to pre-allocate the entire size.
func writeZerosToDst(dst io.Writer, size int64) error {
	// We'll use 1 MB or size, depending on whichever is smaller.
	zeroBufferSize := int64(1024 * 1024)
	if size < zeroBufferSize {
		zeroBufferSize = size
	}
	zeroBytes := make([]byte, zeroBufferSize)
	bytesRemaining := size
	for bytesRemaining > 0 {
		toWriteThisIteration := zeroBufferSize
		if bytesRemaining < toWriteThisIteration {
			toWriteThisIteration = bytesRemaining
		}
		tmp, e := dst.Write(zeroBytes[0:toWriteThisIteration])
		if e != nil {
			return fmt.Errorf("Failed writing zeros: %w", e)
		}
		bytesRemaining -= int64(tmp)
	}
	return nil
}

// Creates a new file to hold the given chunk index. Does not append it to the
// list or modify/access the existing file list.
func (w *ChunkedWriter) createNewFile(index int64) (*os.File, error) {
	newFilename := w.getFilename(index)
	fileFlags := os.O_RDWR | os.O_CREATE
	if !w.OverwriteFiles {
		fileFlags |= os.O_EXCL
	}
	return os.OpenFile(newFilename, fileFlags, 0666)
}

// Writes bytes to the end of the file for the given offset, or until maxSize
// bytes have been written. If we're at the start of a file at the end of
// w.files, this will create a new file (of size chunkSize) and fill it with
// 0s.
func (w *ChunkedWriter) zeroFillFileAtOffset(offset, maxSize int64) (int64,
	error) {
	var e error
	chunkSize := int64(w.chunkSize)
	offsetInFile := offset % chunkSize
	fileIndex := offset / chunkSize
	// Note: we only ever use this function to sequentially append chunks, so
	// this counts as an internal error.
	if fileIndex > int64(len(w.files)) {
		return 0, fmt.Errorf("Internal error: offset %d writes to file %d, "+
			"but only %d files have been created so far", offset, fileIndex,
			len(w.files))
	}
	// Create a new file if we're at the end of the list.
	if fileIndex == int64(len(w.files)) {
		if offsetInFile != 0 {
			return 0, fmt.Errorf("Internal error: Not zero-filling at the " +
				"start of a new file")
		}
		f, e := w.createNewFile(fileIndex)
		if e != nil {
			return 0, fmt.Errorf("Error opening %s: %w",
				w.getFilename(fileIndex), e)
		}
		w.files = append(w.files, f)
	}
	f := w.files[fileIndex]
	bytesRemainingInFile := chunkSize - offsetInFile
	_, e = f.Seek(offsetInFile, io.SeekStart)
	if e != nil {
		return 0, fmt.Errorf("Failed writing zeros to %s: couldn't seek to "+
			"offset %d: %w", w.getFilename(fileIndex), offsetInFile, e)
	}
	toWrite := bytesRemainingInFile
	if toWrite > maxSize {
		toWrite = maxSize
	}
	e = writeZerosToDst(f, toWrite)
	if e != nil {
		return 0, fmt.Errorf("Failed writing zeros to %s: %w",
			w.getFilename(fileIndex), e)
	}
	return toWrite, nil
}

// If w's current offset is past the end of the available files, append 0s (and
// create new files if needed) until we're at the correct offset.
func (w *ChunkedWriter) fillEmpty() error {
	if w.currentOffset < w.maxOffset {
		return nil
	}
	offset := w.maxOffset
	sizeToFill := w.currentOffset - w.maxOffset
	for sizeToFill > 0 {
		bytesWritten, e := w.zeroFillFileAtOffset(offset, sizeToFill)
		if e != nil {
			return fmt.Errorf("Failed filling empty data: %w", e)
		}
		sizeToFill -= bytesWritten
		offset += bytesWritten
	}
	w.maxOffset = w.currentOffset
	return nil
}

func (w *ChunkedWriter) Seek(offset int64, whence int) (int64, error) {
	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = w.currentOffset + offset
	case io.SeekEnd:
		newOffset = w.maxOffset + offset
	default:
		return 0, fmt.Errorf("Invalid Seek \"whence\": %d", whence)
	}
	if newOffset < 0 {
		return 0, fmt.Errorf("Invalid new offset: %d", newOffset)
	}
	w.currentOffset = newOffset
	e := w.fillEmpty()
	if e != nil {
		w.currentOffset = 0
		return 0, fmt.Errorf("Couldn't seek to offset %d: failed "+
			"zero-filling content: %w", newOffset, e)
	}
	return newOffset, nil
}

// Starts writing the beginning of the given "data" slice to the given offset
// in the ChunkedWriter, selecting the appropriate file, and potentially
// creating a new file if necessary.  Will return the number of bytes actually
// written, or an error if one occurred.  This is intended to be called in a
// loop until len(data) is 0. May modify w.files, but will not modify the
// w.offset or w.maxOffset.
func (w *ChunkedWriter) writeNextChunk(offset int64, data []byte) (int64,
	error) {
	if len(data) == 0 {
		return 0, nil
	}
	fileIndex := offset / int64(w.chunkSize)
	offsetInFile := offset % int64(w.chunkSize)
	// The farthest offset we can ever be here is at the start of a new file at
	// the end of the list.
	if fileIndex > int64(len(w.files)) {
		return 0, fmt.Errorf("Internal error: attempting to write more than " +
			"1 chunk past the last available file")
	}
	// Create a new file if we're at the end of the list of existing chunks.
	if fileIndex == int64(len(w.files)) {
		if offsetInFile != 0 {
			return 0, fmt.Errorf("Internal error: attempting to write after " +
				"the start of a chunk that hasn't been created yet")
		}
		f, e := w.createNewFile(int64(len(w.files)))
		if e != nil {
			return 0, fmt.Errorf("Error creating file for chunk %d: %w",
				len(w.files), e)
		}
		w.files = append(w.files, f)
	}
	// We generally have no idea where we last sought in a given file, so we
	// always need to re-seek.
	f := w.files[fileIndex]
	_, e := f.Seek(offsetInFile, io.SeekStart)
	if e != nil {
		return 0, fmt.Errorf("Error seeking to offset %d in chunk %d: %w",
			offsetInFile, fileIndex, e)
	}
	// Compute toWrite to disallow writing more than chunkSize bytes to a file.
	remainingInFile := int64(w.chunkSize) - offsetInFile
	toWrite := remainingInFile
	if toWrite > int64(len(data)) {
		toWrite = int64(len(data))
	}
	bytesWritten, e := f.Write(data[0:toWrite])
	if e != nil {
		tmp := int64(bytesWritten)
		return tmp, fmt.Errorf("Failed writing %d bytes to offset %d in "+
			"chunk %d: %w", toWrite, offsetInFile, fileIndex, e)
	}
	return toWrite, nil
}

func (w *ChunkedWriter) Write(data []byte) (int, error) {
	// We'll simply write chunks of data in a loop until we have no more left.
	// We'll only modify w's state minimally until writes are finished.
	offset := w.currentOffset
	totalWritten := int64(0)
	for len(data) > 0 {
		bytesWritten, e := w.writeNextChunk(offset, data)
		if e != nil {
			w.currentOffset = offset
			if offset > w.maxOffset {
				w.maxOffset = offset
			}
			return int(totalWritten), e
		}
		totalWritten += bytesWritten
		offset += bytesWritten
		data = data[bytesWritten:]
	}
	// Now that we've successfully written everything we'll update the current
	// and maxOffset.
	w.currentOffset = offset
	if offset > w.maxOffset {
		w.maxOffset = offset
	}
	return int(totalWritten), nil

}

// Creates a new ChunkedWriter, which will be backed by files named
// <baseFilename>.part.<chunkNumber>. Each chunk will be chunkSize bytes,
// apart from the last chunk, which may be smaller. Note that no files will be
// created or written until the first Read() or Seek() operation.
func NewChunkedWriter(chunkSize uint64, baseFilename string) (*ChunkedWriter,
	error) {
	return &ChunkedWriter{
		OverwriteFiles: true,
		chunkSize:      chunkSize,
		baseFilename:   baseFilename,
		files:          make([]*os.File, 0, 32),
		currentOffset:  0,
		maxOffset:      0,
	}, nil
}

// We delegate management of file handles to this struct, so the actual
// ChunkedReader is decoupled from how we cache (or don't cache) open files.
type fileChunkSource struct {
	// The open handles for each file.
	files []*os.File
	// The size of each file chunk
	fileSizes []int64
	// The cumulative size of each chunk, used to binary-search for the chunk
	// containing a given offset. Index i of this slice contains the offset at
	// which file i *starts*.
	cumulativeSizes []int64
	// The index of lastOpenedFile.
	lastOpenedFileIndex int64
}

// Returns the total size of all files provided by this source.
func (s *fileChunkSource) Size() int64 {
	if len(s.files) == 0 {
		return 0
	}
	n := len(s.files)
	return s.cumulativeSizes[n-1] + s.fileSizes[n-1]
}

// Closes any underlying file handles. Must be called when the file chunks are
// no longer needed.
func (s *fileChunkSource) Close() error {
	var e, tmp error
	for i := range s.files {
		tmp = s.files[i].Close()
		// We'll keep track only of the first error we encounter, but continue
		// trying to close everything.
		if (tmp != nil) && (e == nil) {
			e = tmp
		}
	}
	return e
}

// Returns the file containing the given offset, followed by the offset within
// the file and the file's total size.
func (s *fileChunkSource) GetFileForOffset(offset int64) (*os.File, int64,
	int64, error) {
	if len(s.files) == 0 {
		return nil, 0, 0, fmt.Errorf("No files are available")
	}
	// Quick optimization under the assumption that the first chunk is more
	// likely to be accessed frequently.
	if offset < s.fileSizes[0] {
		return s.files[0], offset, s.fileSizes[0], nil
	}
	// Second basic heuristic: check whether we're still in the same file as
	// last time.
	if s.lastOpenedFileIndex >= 0 {
		lastOpenedStart := s.cumulativeSizes[s.lastOpenedFileIndex]
		lastOpenedSize := s.fileSizes[s.lastOpenedFileIndex]
		lastOpenedEnd := lastOpenedStart + lastOpenedSize
		if (offset >= lastOpenedStart) && (offset < lastOpenedEnd) {
			return s.files[s.lastOpenedFileIndex], offset - lastOpenedStart,
				lastOpenedSize, nil
		}
	}
	// Use go's standard sort to find the index containing the given offset.
	// The binary sort is fine, because the cumulative sizes are inherently
	// sorted.
	index, _ := sort.Find(len(s.cumulativeSizes), func(i int) int {
		rangeStart := s.cumulativeSizes[i]
		if rangeStart < offset {
			return 1
		}
		if rangeStart == offset {
			return 0
		}
		return -1
	})
	if index >= len(s.cumulativeSizes) {
		return nil, 0, 0, fmt.Errorf("Internal error: GetFileForOffset "+
			"called with an offset of %d, while the total size is %d",
			offset, s.Size())
	}
	offsetInFile := offset - s.cumulativeSizes[index]
	fileSize := s.fileSizes[index]
	s.lastOpenedFileIndex = int64(index)
	return s.files[index], offsetInFile, fileSize, nil
}

// Returns a new file chunk source. The returned fileChunkSource must be
// Close()'d by the caller when no longer needed.
func newFileChunkSource(filePaths ...string) (*fileChunkSource, error) {
	if len(filePaths) == 0 {
		return nil, fmt.Errorf("No file paths were provided")
	}
	files := make([]*os.File, len(filePaths))
	fileSizes := make([]int64, len(filePaths))
	cumulativeSizes := make([]int64, len(filePaths))
	totalSize := int64(0)
	var e error
	for i, filePath := range filePaths {
		files[i], e = os.Open(filePath)
		if e != nil {
			return nil, fmt.Errorf("Failed opening %s: %w", filePath, e)
		}
		info, e := files[i].Stat()
		if e != nil {
			return nil, fmt.Errorf("Failed getting info for %s: %w", filePath,
				e)
		}
		// Note that the cumulative size at a given index is the size *prior*
		// to that index.
		cumulativeSizes[i] = totalSize
		fileSizes[i] = info.Size()
		totalSize += fileSizes[i]
	}
	return &fileChunkSource{
		files:               files,
		fileSizes:           fileSizes,
		cumulativeSizes:     cumulativeSizes,
		lastOpenedFileIndex: -1,
	}, nil
}

// Satisfies the ReadSeeker interface, in order to allow reading from multiple
// files as if they're a single contiguous file. Create instances of this using
// NewChunkedReader. The caller must call Close() on this when it's no longer
// needed.
type ChunkedReader struct {
	files *fileChunkSource
	// The current offset from which we're reading
	currentOffset int64
	// The maximum offset, i.e., the total "size" of all chunks.
	size int64
}

// Returns a new ChunkedReader instance, requiring the paths to each subsequent
// chunk, starting with chunk 0. Note that the filePaths provided to this
// function do not need to adhere to any naming convention, or necessarily be
// the same size.
func NewChunkedReader(filePaths ...string) (*ChunkedReader, error) {
	fileSource, e := newFileChunkSource(filePaths...)
	if e != nil {
		return nil, fmt.Errorf("Error accessing chunk files: %w", e)
	}
	return &ChunkedReader{
		files:         fileSource,
		currentOffset: 0,
		size:          fileSource.Size(),
	}, nil
}

func (r *ChunkedReader) Close() error {
	return r.files.Close()
}

func (r *ChunkedReader) Seek(offset int64, whence int) (int64, error) {
	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = r.currentOffset + offset
	case io.SeekEnd:
		newOffset = r.size + offset
	default:
		return 0, fmt.Errorf("Invalid Seek \"whence\": %d", whence)
	}
	if newOffset < 0 {
		return 0, fmt.Errorf("Invalid new offset: %d", newOffset)
	}
	if newOffset > r.size {
		r.currentOffset = r.size
		return r.size, fmt.Errorf("New offset (%d) larger than file size "+
			"(%d): %w", newOffset, r.size, io.EOF)
	}
	r.currentOffset = newOffset
	return newOffset, nil
}

// Reads as much as possible from a single chunk into dst. Returns the number
// of bytes read, or an error if any occurs. Takes the current offset, rather
// than using r.currentOffset to enable changing internal state on an error.
func (r *ChunkedReader) readSingleChunk(dst []byte,
	currentOffset int64) (int64, error) {
	f, offsetInFile, fileSize, e := r.files.GetFileForOffset(currentOffset)
	if e != nil {
		return 0, fmt.Errorf("Error getting chunk for reading offset %d: %w",
			currentOffset, e)
	}
	_, e = f.Seek(offsetInFile, io.SeekStart)
	if e != nil {
		return 0, fmt.Errorf("Error seeking to offset %d (%d in file) "+
			"when reading: %w", currentOffset, offsetInFile, e)
	}
	remainingInFile := fileSize - offsetInFile
	bytesToRead := int64(len(dst))
	if bytesToRead > remainingInFile {
		bytesToRead = remainingInFile
	}
	_, e = f.Read(dst[:bytesToRead])
	if e != nil {
		return 0, fmt.Errorf("Error reading %d bytes at offset %d (%d in "+
			"file): %w", bytesToRead, currentOffset, offsetInFile, e)
	}
	return bytesToRead, nil
}

func (r *ChunkedReader) Read(dst []byte) (int, error) {
	if r.currentOffset >= r.size {
		return 0, io.EOF
	}
	currentOffset := r.currentOffset
	totalRead := int64(0)
	for len(dst) > 0 {
		if currentOffset >= r.size {
			r.currentOffset = currentOffset
			return int(totalRead), io.EOF
		}
		bytesRead, e := r.readSingleChunk(dst, currentOffset)
		totalRead += bytesRead
		currentOffset += bytesRead
		if e != nil {
			r.currentOffset = currentOffset
			return int(totalRead), fmt.Errorf("Failed reading chunk: %w", e)
		}
		dst = dst[bytesRead:]
	}
	r.currentOffset = currentOffset
	return int(totalRead), nil
}
