package celery

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"os"
	"path"
	"strings"
	"time"
)

var (
	l        *zap.SugaredLogger
	levelMap = map[string]zapcore.Level{
		"debug":  zapcore.DebugLevel,
		"info":   zapcore.InfoLevel,
		"warn":   zapcore.WarnLevel,
		"error":  zapcore.ErrorLevel,
		"dpanic": zapcore.DPanicLevel,
		"panic":  zapcore.PanicLevel,
		"fatal":  zapcore.FatalLevel,
	}
	consoleEncoderConfig = zapcore.EncoderConfig{
		TimeKey:       "time",
		LevelKey:      "level",
		NameKey:       "logger",
		CallerKey:     "file",
		MessageKey:    "msg",
		StacktraceKey: "stacktrace",
		LineEnding:    zapcore.DefaultLineEnding,
		EncodeLevel:   zapcore.CapitalColorLevelEncoder,
		EncodeTime: func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
			enc.AppendString(t.Format("2006-01-02 15:04:05"))
		},
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
		EncodeName:     zapcore.FullNameEncoder,
	}
	jsonEncoderConfig = zapcore.EncoderConfig{
		TimeKey:       "time",
		LevelKey:      "level",
		NameKey:       "logger",
		CallerKey:     "file",
		MessageKey:    "msg",
		StacktraceKey: "stacktrace",
		LineEnding:    zapcore.DefaultLineEnding,
		EncodeLevel:   zapcore.CapitalLevelEncoder,
		EncodeTime: func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
			enc.AppendString(t.Format("2006-01-02 15:04:05"))
		},
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
		EncodeName:     zapcore.FullNameEncoder,
	}
)

func toLevel(lev string) zapcore.Level {
	if level, ok := levelMap[strings.ToLower(lev)]; ok {
		return level
	}
	return zapcore.InfoLevel
}

func newLog(appName string, cfg logConfig) *zap.SugaredLogger {
	fileName := path.Join(cfg.Path, cfg.File)
	hook := lumberjack.Logger{
		Filename:   fileName,
		MaxSize:    128,
		MaxBackups: 30,
		MaxAge:     7,
		Compress:   true,
	}

	level := toLevel(cfg.Level)

	loggerLevel := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl >= level
	})

	core := zapcore.NewTee(
		zapcore.NewCore(zapcore.NewConsoleEncoder(consoleEncoderConfig), zapcore.AddSync(os.Stdout), loggerLevel),
		zapcore.NewCore(zapcore.NewJSONEncoder(jsonEncoderConfig), zapcore.AddSync(&hook), loggerLevel),
	)

	fileds := zap.Fields(zap.String("app", appName))

	logger := zap.New(core, zap.AddCaller(), fileds)
	logger.Info("log inited.")
	l := logger.Sugar()
	defer logger.Sync()
	return l
}

// GetLogger if you want log something, use it
func GetLogger(name string) *zap.SugaredLogger {
	return l.Named(name)
}
