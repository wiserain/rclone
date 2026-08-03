package _115 // nolint:revive

import (
	"time"

	"github.com/rclone/rclone/backend/115/api"
)

type apiErrorAction uint8

const (
	apiErrorUnhandled apiErrorAction = iota
	apiErrorRetry
	apiErrorFatal
	apiErrorObjectNotFound
	apiErrorNoRetry
)

type apiErrorClassification struct {
	action apiErrorAction
	delay  time.Duration
}

// classifyAPIError returns how an API error should be handled.
func classifyAPIError(code api.Int) apiErrorClassification {
	switch code {
	case 990009:
		// 删除[subdir]操作尚未执行完成，请稍后再试！
		// Delete [subdir] is still in progress; try again later.
		// 还原[...]操作尚未执行完成，请稍后再试！
		// Restore [...] is still in progress; try again later.
		// 复制[...]操作尚未执行完成，请稍后再试！
		// Copy [...] is still in progress; try again later.
		return apiErrorClassification{action: apiErrorRetry, delay: time.Second}
	case 990019:
		// 移动[...]操作尚未执行完成，请稍后再试！
		// Move [...] is still in progress; try again later.
		return apiErrorClassification{action: apiErrorRetry, delay: time.Second}
	case 590075:
		// 操作太频繁，请稍候再试
		// Operation too frequent; try again later.
		return apiErrorClassification{action: apiErrorRetry}
	case 990005:
		// 你的账号有类似任务正在处理，请稍后再试！
		// Your account has a similar task in progress; try again later.
		return apiErrorClassification{action: apiErrorRetry}
	case 40110000:
		// 请求异常需要重试
		// Abnormal request; retry required.
		return apiErrorClassification{action: apiErrorRetry}
	case 99:
		// 请重新登录
		// Please log in again.
		return apiErrorClassification{action: apiErrorFatal}
	case 990001:
		// 登陆超时，请重新登陆。
		// Login timed out; please log in again.
		return apiErrorClassification{action: apiErrorFatal}
	case 40101032:
		// 请重新登录
		// Please log in again.
		return apiErrorClassification{action: apiErrorFatal}
	case 40101035, 40101037:
		return apiErrorClassification{action: apiErrorFatal}
	case 50001:
		return apiErrorClassification{action: apiErrorObjectNotFound}
	case 50003:
		// 很抱歉，该文件提取码不存在。
		// Sorry, this file's pickcode does not exist.
		return apiErrorClassification{action: apiErrorObjectNotFound}
	case 50015:
		// 文件不存在或已删除。
		// File does not exist or has been deleted.
		return apiErrorClassification{action: apiErrorObjectNotFound}
	case 70005:
		// 文件不存在或已删除
		// File does not exist or has been deleted.
		return apiErrorClassification{action: apiErrorObjectNotFound}
	case 231011:
		// 文件已删除，请勿重复操作
		// File has already been deleted; do not repeat the operation.
		return apiErrorClassification{action: apiErrorObjectNotFound}
	case 50028:
		// 文件大小超出限制，请使用115电脑端下载
		// File exceeds the size limit; use the 115 desktop client to download it.
		return apiErrorClassification{action: apiErrorNoRetry}
	case 91002:
		// 不能将文件复制到自身或其子目录下。
		// A file cannot be copied to itself or its subdirectory.
		return apiErrorClassification{action: apiErrorNoRetry}
	case 91005:
		// 空间不足，复制失败。
		// Insufficient space; copy failed.
		return apiErrorClassification{action: apiErrorNoRetry}
	case 800006, 4100009:
		return apiErrorClassification{action: apiErrorNoRetry}
	case 4100026:
		// 该文件分享链接不存在或已被删除
		// This file share link does not exist or has been deleted.
		return apiErrorClassification{action: apiErrorNoRetry}
	default:
		return apiErrorClassification{}
	}
}
