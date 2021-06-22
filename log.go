package celery

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"os"
	"path"
)

var (
	L *zap.Logger
)

func NewLog(cfg logConfig) *zap.Logger {
	fileName := path.Join(cfg.Path, cfg.File)
	hook := lumberjack.Logger{
		Filename:   fileName, // 日志文件路径
		MaxSize:    128,      // 每个日志文件保存的最大尺寸 单位：M
		MaxBackups: 30,       // 日志文件最多保存多少个备份
		MaxAge:     7,        // 文件最多保存多少天
		Compress:   true,     // 是否压缩
	}

	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "time",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "file",
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,  // 小写编码器
		EncodeTime:     zapcore.ISO8601TimeEncoder,     // ISO8601 UTC 时间格式
		EncodeDuration: zapcore.SecondsDurationEncoder, //
		EncodeCaller:   zapcore.ShortCallerEncoder,     // 全路径编码器
		EncodeName:     zapcore.FullNameEncoder,
	}
	var level zapcore.Level
	switch cfg.Level {
	case "debug", "DEBUG":
		level = zap.DebugLevel
	case "info", "INFO":
		level = zap.InfoLevel
	case "warn", "WARN":
		level = zap.WarnLevel
	case "error", "ERROR":
		level = zap.ErrorLevel
	case "panic", "PANIC":
		level = zap.DPanicLevel
	}

	loggerLevel := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl >= level
	})

	core := zapcore.NewCore(
		zapcore.NewConsoleEncoder(encoderConfig),                                        // 编码器配置
		zapcore.NewMultiWriteSyncer(zapcore.AddSync(os.Stdout), zapcore.AddSync(&hook)), // 打印到控制台和文件
		loggerLevel, // 日志级别
	)

	// 开启开发模式，堆栈跟踪
	caller := zap.AddCaller()
	// 开启文件及行号
	development := zap.Development()
	// 设置初始化字段
	filed := zap.Fields(zap.String("serviceName", "serviceName"))
	// 构造日志
	logger := zap.New(core, caller, development, filed)

	logger.Info("log 初始化成功")
	return logger
}

func GetLogger() *zap.Logger {
	return L
}
