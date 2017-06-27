package handlers

import (
	"bufio"
	"os"
	"path/filepath"

	"bytes"
	"errors"
	"io"
)

// FileSource 文件源，按行读取。
type FileSource struct {
	file *os.File
	r    *bufio.Reader
}

// NewFileSrc 新建文件源
func NewFileSrc(filePath string) (*FileSource, error) {
	file, err := os.OpenFile(filePath, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return nil, err
	}
	return &FileSource{
		file: file,
		r:    bufio.NewReader(file),
	}, nil
}

// Next 实现 Source 接口。
func (fs *FileSource) Next() (data interface{}, err error) {
	line, err := fs.r.ReadString('\n')
	if err != nil {
		fs.Close()
	}
	return line, err
}

// Close 关闭文件，可以主动关闭，调用 Next 的过程中如果产生错误会自动关闭。
func (fs *FileSource) Close() error {
	if fs.file != nil {
		err := fs.file.Close()
		fs.file = nil
		return err
	}
	return nil
}

// MultiFileSrc 多文件源
type MultiFileSrc struct {
	src   []*FileSource
	index int
}

// NewMultiFileSrc 创建多文件源，filesPattern 的意义和 filepath.Glob 相同。
func NewMultiFileSrc(filesPattern string) (*MultiFileSrc, error) {
	files, err := filepath.Glob(filesPattern)
	if err != nil {
		return nil, err
	}
	mfs := &MultiFileSrc{}
	mfs.src = make([]*FileSource, len(files))
	for i, file := range files {
		mfs.src[i], err = NewFileSrc(file)
		if err != nil {
			mfs.Close()
			return nil, err
		}
	}
	return mfs, nil
}

// Next 实现 Source 接口。
func (mfs *MultiFileSrc) Next() (data interface{}, err error) {
	if mfs.index >= len(mfs.src) {
		return nil, io.EOF
	}
	data, err = mfs.src[mfs.index].Next()
	if err != nil {
		mfs.index++
		if err == io.EOF && mfs.index < len(mfs.src) {
			err = nil
		}
	}
	return
}

// Close 关闭。
func (mfs *MultiFileSrc) Close() error {
	errBuf := bytes.Buffer{}
	for _, src := range mfs.src {
		if src == nil {
			continue
		}
		err := src.Close()
		if err != nil {
			errBuf.WriteString(err.Error())
		}
	}
	if errBuf.Len() > 0 {
		return errors.New(errBuf.String())
	}
	return nil
}
