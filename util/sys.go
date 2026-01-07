package util

import (
	"runtime"
	"syscall"
	"unsafe"
)

// SysOpenatArm64 macOS 系统调用号（ARM64 架构）
const SysOpenatArm64 = 463

func Openat(dirFd int, path string, flags int, perm uint32) (int, error) {
	// Prefer the standard library implementation when available.
	if runtime.GOOS != "darwin" || runtime.GOARCH != "arm64" {
		return syscall.Openat(dirFd, path, flags, perm)
	}

	pathPtr, err := syscall.BytePtrFromString(path)
	if err != nil {
		return -1, err
	}

	r1, _, e := syscall.Syscall6(
		SysOpenatArm64,
		uintptr(dirFd),
		uintptr(unsafe.Pointer(pathPtr)),
		uintptr(flags),
		uintptr(perm),
		0,
		0,
	)

	if e != 0 {
		return -1, e
	}
	return int(r1), nil
}
