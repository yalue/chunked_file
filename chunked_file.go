// The chunked_file library wraps multiple on-disk file chunks, implementing
// the io.ReadSeeker and io.WriteSeeker interfaces backed by multiple
// underlying files.
package chunked_file

import (
	"io"
	"os"
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
			msg += ", " + e.Error
		}
	}
	e.files = nil
	e.fileNames = nil
	e.baseFilename = ""
	e.chunkSize = 0
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
	return fmt.Sprintf("%s.%d.part", w.baseFilename, chunkIndex)
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
func (w *ChunkedWriter) createNewFile(index int) (*os.File, error) {
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
	if fileIndex > len(w.files) {
		return 0, fmt.Errorf("Internal error: offset %d writes to file %d, "+
			"but only %d files have been created so far", offset, fileIndex,
			len(w.files))
	}
	// Create a new file if we're at the end of the list.
	if fileIndex == len(w.files) {
		if offsetInFile != 0 {
			return 0, fmt.Errorf("Internal error: Not zero-filling at the " +
				"start of a new file")
		}
		w.files[fileIndex], e = w.createNewFile(fileIndex)
		if e != nil {
			return 0, fmt.Errorf("Error opening %s: %w", newFilename, e)
		}
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
	maxSize := w.maxOffset - w.currentOffset
	for maxSize > 0 {
		bytesWritten, e := w.zeroFillFileAtOffset(offset, maxSize)
		if e != nil {
			return fmt.Errorf("Failed filling empty data: %w", e)
		}
		maxSize -= bytesWritten
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
	oldOffset := w.currentOffset
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
		f, e := w.createNewFile(len(w.files))
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
		return bytesWritten, fmt.Errorf("Failed writing %d bytes to offset "+
			"%d in chunk %d: %w", toWrite, offsetInFile, fileIndex, e)
	}
	return toWrite, nil
}

func (w *ChunkedWriter) Write(data []byte) (int, error) {
	// We'll simply write chunks of data in a loop until we have no more left.
	// We'll only modify w's state minimally until writes are finished.
	offset := w.currentOffset
	totalWritten := 0
	for len(data) > 0 {
		bytesWritten, e := w.writeNextChunk(offset, data)
		if e != nil {
			// NOTE: Should I also update currentOffset and maxOffset here,
			// if a write has failed?
			return totalWritten, e
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
// <baseFilename>.<chunkNumber>.part. Each chunk will be chunkSize bytes,
// apart from the last chunk, which may be smaller. Note that no files will be
// created or written until the first Read() or Seek() operation.
func NewChunkedWriter(chunkSize uint64, baseFilename string) *ChunkedWriter {
	return &ChunkedWriter{
		OverwiteFiles: true,
		chunkSize:     chunkSize,
		baseFilename:  baseFilename,
		files:         make([]*os.File, 0, 32),
		currentOffset: 0,
		maxOffset:     0,
	}
}

// We delegate management of file handles to this struct, so the actual
// ChunkedReader is decoupled from how we cache (or don't cache) open files.
type fileChunkSource struct {
	// A list of all of the filenames for the file chunks.
	filePaths []string
	// The size of each file chunk
	fileSizes []int64
	// The total size of all chunks. Equal to the sum of fileSizes.
	totalSize int64
	// The last file that was opened; will be closed internally when a new
	// chunk is opened.
	lastOpenedFile *os.File
}

// Returns the total size of all files provided by this source.
func (s *fileChunkSource) Size() (int64, error) {
	return s.totalSize, nil
}

// Closes any underlying file handles. Must be called when the file chunks are
// no longer needed.
func (s *fileChunkSource) Close() error {
	if s.lastOpenedFile == nil {
		return nil
	}
	return s.lastOpenedFile.Close()
}

// Returns the file containing the given offset, followed by the offset within
// the file and the file's total size.
func (s *fileChunkSource) GetFileForOffset(offset int64) (*os.File, int64,
	int64, error) {
	return nil, 0, 0, fmt.Errorf("Not yet implemented")
}

// Returns a new file chunk source. The returned fileChunkSource must be
// Close()'d by the caller when no longer needed.
func newFileChunkSource(filePaths ...string) (*fileChunkSource, error) {
	fileSizes := make([]int64, len(filePaths))
	for i, filePath := range filePaths {
		f, e := os.Open(filePath)
		if e != nil {
			return nil, fmt.Errorf("Failed opening %s: %w", filePath, e)
		}
		info, e := f.Stat()
		f.Close()
		if e != nil {
			return nil, fmt.Errorf("Failed getting info for %s: %w", filePath,
				e)
		}
		fileSizes[i] = info.Size()
	}
	totalSize := int64(0)
	for _, size := range fileSizes {
		totalSize += size
	}
	return &fileChunkSource{
		filePaths:      filePaths,
		fileSizes:      fileSizes,
		totalSize:      totalSize,
		lastOpenedFile: nil,
	}, nil
}

// TODO (next): Finish changing fileChunkSource to a struct.

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

// TODO (next, 2): Implement ChunkedReader's Read function:
//  - In a loop, call a readNextChunk function, which will get the chunk source
//    for the current offset and read until the end of it, or until the
//    remaining needed data has been provided.

// TODO: Write tests for ChunkedReader and ChunkedWriter.
