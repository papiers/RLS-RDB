package util

import (
	"syscall"
	"unsafe"
)

// SysOpenatArm64 macOS 系统调用号（ARM64 架构）
const SysOpenatArm64 = 463

func Openat(dirFd int, path string, flags int, perm uint32) (int, error) {
	// 将路径转换为 C 风格字符串
	pathPtr, err := syscall.BytePtrFromString(path)
	if err != nil {
		return -1, err
	}

	// 调用系统调用
	r1, _, e := syscall.Syscall6(
		SysOpenatArm64,
		uintptr(dirFd),
		uintptr(unsafe.Pointer(pathPtr)),
		uintptr(flags),
		uintptr(perm),
		0, // 未使用
		0, // 未使用
	)

	if e != 0 {
		return -1, e
	}
	return int(r1), nil
}
