Transparently Read or Write Using Multiple Files in Go
======================================================

The `chunked_file` package implements the `io.ReadSeeker` and `io.WriteSeeker`
interface, backed by multiple underlying files on disk.

Usage
-----

As you write to a `ChunkedWriter` instance, multiple files are automatically
created on disk, each being up to a user-specified chunk size.  Users can also
create a `ChunkedReader` using a list of filenames, which will transparently
provide data from each underlying file as if they are a single contiguous file.

[Online documentation on pkg.go.dev.](https://pkg.go.dev/github.com/yalue/chunked_file)

Example:

```go
import (
    "github.com/yalue/chunked_file"
    "path/filepath"
)

func main() {
    // As we write to this, automatically create 4KB files named
    // my_data.part.0, my_data.part.1, etc.
    chunkedWriter, _ := chunked_file.NewChunkedWriter(4096, "my_data")

    // ... Use the chunkedWriter as you would any normal io.WriteSeeker. Note
    // that seeking ahead may zero-fill any prior unwritten data.

    // Closing the chunkedWriter will close all underlying files.
    chunkedWriter.Close()

    // We can use filepath.Glob to obtain a list of all of the .part files
    // created by the chunkedWriter, and read them using a ChunkedReader.
    filePaths, _ := filepath.Glob("my_data.part.*")
    chunkedReader, _ := chunked_file.NewChunkedReader(filePaths...)

    // ... Use the chunkedReader as you would any normal io.ReadSeeker.

    // As with the writer, closing the reader closes all underlying files.
    chunkedReader.Close()
}
```

