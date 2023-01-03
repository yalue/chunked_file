package chunked_file

import (
	"bytes"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

func TestChunkedReader(t *testing.T) {
	r, e := NewChunkedReader("test_data/part1.txt", "test_data/part2.txt",
		"test_data/part3.txt")
	if e != nil {
		t.Logf("Failed opening chunked reader: %s\n", e)
		t.FailNow()
	}
	totalSize, e := r.Seek(0, io.SeekEnd)
	if e != nil {
		t.Logf("Failed seeking to end of chunked reader: %s\n", e)
		t.FailNow()
	}
	if totalSize != 17 {
		t.Logf("Got incorrect combined size: Expected %d bytes, got %d\n",
			17, totalSize)
		t.FailNow()
	}
	_, e = r.Seek(0, io.SeekStart)
	if e != nil {
		t.Logf("Failed rewinding to start of reader: %s\n", e)
		t.FailNow()
	}

	// We'll intentionally oversize this buffer, to ensure that we only read
	// the correct amount and get an EOF.
	dstBuffer := [45]byte{}
	_, e = r.Seek(3, io.SeekStart)
	if e != nil {
		t.Logf("Failed seeking to offset 3 in reader: %s\n", e)
		t.FailNow()
	}
	bytesRead, e := r.Read(dstBuffer[0:5])
	if e != nil {
		t.Logf("Failed reading 5 bytes from reader: %s\n", e)
		t.FailNow()
	}
	if bytesRead != 5 {
		t.Logf("Incorrect amount returned by reader.Read: %d\n", bytesRead)
		t.FailNow()
	}
	if string(dstBuffer[:5]) != "there" {
		t.Logf("Incorrect Read result, got %s\n", dstBuffer[:5])
		t.FailNow()
	}
	_, e = r.Seek(0, io.SeekStart)
	if e != nil {
		t.Logf("Failed rewinding (second time): %s\n", e)
		t.FailNow()
	}
	bytesRead, e = r.Read(dstBuffer[:])
	if e != io.EOF {
		t.Logf("Didn't get expected EOF error.\n")
		t.Fail()
		if e != nil {
			t.Logf("Got unexpected non-EOF error: %s\n", e)
			t.FailNow()
		}
	}
	t.Logf("Got expected EOF error: %s\n", e)
	if int64(bytesRead) != totalSize {
		t.Logf("Bad full read size. Expected %d, got %d\n", totalSize,
			bytesRead)
		t.FailNow()
	}
	if string(dstBuffer[:17]) != "Hi there, friend!" {
		t.Logf("Got incorrect Read() content: %s\n", dstBuffer[:17])
		t.FailNow()
	}
}

// Used for cleaning up test data, doesn't cause failures itself, but will log
// any deletion errors that occur.
func deleteAllFiles(filenames []string, t *testing.T) {
	for _, f := range filenames {
		e := os.Remove(f)
		if e != nil {
			t.Logf("Failed deleting %s: %s\n", f, e)
		}
	}
}

// Opens, reads, and returns the content of the file at the given path. Causes
// a test failure on error. Closes the file before returning.
func getFileContent(path string, t *testing.T) []byte {
	f, e := os.Open(path)
	if e != nil {
		t.Logf("Failed opening %s: %s\n", path, e)
		t.FailNow()
	}
	toReturn, e := io.ReadAll(f)
	if e != nil {
		t.Logf("Failed reading %s: %s\n", path, e)
		f.Close()
		t.FailNow()
	}
	f.Close()
	return toReturn
}

func TestChunkedWriter(t *testing.T) {
	rng := rand.New(rand.NewSource(1337))
	testData := make([]byte, 512)
	pathPrefix := "test_data/test_output"
	for i := range testData {
		testData[i] = byte(rng.Int())
	}
	w, e := NewChunkedWriter(100, pathPrefix)
	if e != nil {
		t.Logf("Failed creating ChunkedWriter: %s\n", e)
		t.FailNow()
	}
	offset, e := w.Seek(210, io.SeekStart)
	if e != nil {
		t.Logf("Failed seeking to offset 210: %s\n", e)
		t.FailNow()
	}
	if offset != 210 {
		t.Logf("Seek to offset 210 returned %d instead\n", offset)
		t.FailNow()
	}

	bytesWritten, e := w.Write(testData)
	if e != nil {
		t.Logf("Failed writing test data: %s\n", e)
		t.FailNow()
	}
	if bytesWritten != len(testData) {
		t.Logf("Expected to write %d bytes, but Write() returned %d\n",
			len(testData), bytesWritten)
		t.FailNow()
	}
	filenames, e := filepath.Glob("test_data/test_output.part.*")
	if e != nil {
		t.Logf("Failed getting output filenames: %s\n", e)
		t.FailNow()
	}
	if len(filenames) != 8 {
		t.Logf("Expected to get 8 part files, but got %d\n", len(filenames))
		deleteAllFiles(filenames, t)
		t.FailNow()
	}

	// Make sure the last file wasn't padded, and that it contains the final
	// bytes written.
	fileContent := getFileContent(filenames[7], t)
	if len(fileContent) != 22 {
		t.Logf("Expected %s to contain 22 bytes, but got %d\n",
			filenames[7], len(fileContent))
		deleteAllFiles(filenames, t)
		t.FailNow()
	}
	if !bytes.Equal(fileContent, testData[490:]) {
		t.Logf("Didn't get expected test data content in last file. Expected"+
			"'% x', got '% x'\n", testData[490:], fileContent)
		t.FailNow()
	}

	// Make sure the second file only contains 0s
	fileContent = getFileContent(filenames[1], t)
	if len(fileContent) != 100 {
		t.Logf("%d-byte file %s doens't match the chunk size of 100\n",
			len(fileContent), filenames[1])
		deleteAllFiles(filenames, t)
		t.FailNow()
	}
	for i, b := range fileContent {
		if b != 0 {
			t.Logf("Found non-zero byte at offset %d of %s\n", i, filenames[1])
			deleteAllFiles(filenames, t)
			t.FailNow()
		}
	}

	// Make sure the third file contains the content we expect, after the 10
	// bytes of additional padding we inserted before it by the 210-byte seek.
	fileContent = getFileContent(filenames[2], t)
	for i, b := range fileContent[:10] {
		if b != 0 {
			t.Logf("Found non-zero byte at offset %d of %s\n", i, filenames[2])
			deleteAllFiles(filenames, t)
			t.FailNow()
		}
	}
	if !bytes.Equal(fileContent[10:], testData[0:90]) {
		t.Logf("First 90 bytes of test data don't match %s content (expected"+
			"% x..., got % x...)\n", filenames[2], testData[0:10],
			fileContent[10:20])
		deleteAllFiles(filenames, t)
		t.FailNow()
	}

	// Quick test to make sure we can't overwrite existing files with a new
	// writer.
	tmpWriter, _ := NewChunkedWriter(100, "test_data/test_output")
	tmpWriter.OverwriteFiles = false
	_, e = tmpWriter.Seek(1, io.SeekStart)
	if e == nil {
		t.Logf("Didn't get expected error when overwriting existing files\n")
		// Remember to call deleteAllFiles if we ever change this to FailNow()
		t.Fail()
	}
	t.Logf("Got expected error when overwriting existing files: %s\n", e)
	tmpWriter.Close()

	// Remove the remaining files.
	deleteAllFiles(filenames, t)
	tmpWriter = nil
}
