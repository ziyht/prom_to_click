package modules

import (
	"github.com/natefinch/lumberjack"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"path/filepath"
	"time"
)

func myTimeEncoder(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(t.Format("2006-01-02_15:04:05.000"))
}


func getEncoder() zapcore.Encoder {
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime  = myTimeEncoder 	// zapcore.ISO8601TimeEncoder
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	return zapcore.NewConsoleEncoder(encoderConfig)
}

func getLogWriter(path string) zapcore.WriteSyncer {
	lumberJackLogger := &lumberjack.Logger{
		Filename:   path,
		MaxSize:    Cfg.Logger.MaxSize,
		MaxBackups: Cfg.Logger.MaxBackups,
		MaxAge:     30,
		Compress:   false,
	}
	return zapcore.AddSync(lumberJackLogger)
}

var slog *zap.SugaredLogger

func GetLog() *zap.SugaredLogger{
	return slog
}

func initLogger() {

	var logger *zap.Logger

	//curDir := filepath.Dir(os.Args[0])
	//curDir, _ = filepath.Abs(curDir)

	if Cfg.Logger.Dir == ""{
		Cfg.Logger.Dir = filepath.Join("var/log")
	}
	if Cfg.Logger.MaxSize < 1 {
		Cfg.Logger.MaxSize = 100
	}
	if Cfg.Logger.MaxBackups < 1 {
		Cfg.Logger.MaxBackups = 7
	}
	if Cfg.Logger.MaxAge < 1 {
		Cfg.Logger.MaxSize = 7
	}

	levelConsole := zapcore.InfoLevel
	levelFile    := zapcore.InfoLevel

	switch Cfg.Logger.LevelConsole {
		case "debug" : levelConsole = zapcore.DebugLevel
		case "info"  : levelConsole = zapcore.InfoLevel
		case "warn"  : levelConsole = zapcore.WarnLevel
		case "error" : levelConsole = zapcore.ErrorLevel
	}

	switch Cfg.Logger.LevelFile {
		case "debug" : levelFile = zapcore.DebugLevel
		case "info"  : levelFile = zapcore.InfoLevel
		case "warn"  : levelFile = zapcore.WarnLevel
		case "error" : levelFile = zapcore.ErrorLevel
	}

	hostname, _ := os.Hostname()
	path := Cfg.Logger.Dir + "/" + hostname + "_prom_to_click.log"

	coreConsole := zapcore.NewCore(getEncoder(), zapcore.AddSync(os.Stdout), levelConsole)
    coreFile    := zapcore.NewCore(getEncoder(), getLogWriter(path), levelFile)

	logger = zap.New(zapcore.NewTee(coreConsole, coreFile))

	defer logger.Sync() 		// flushes buffer, if any
	slog = logger.Sugar()
}
