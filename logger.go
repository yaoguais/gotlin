package gotlin

import (
	"fmt"

	"github.com/sirupsen/logrus"
)

var stdLogger = logrus.StandardLogger()

func init() {
	level, err := logrus.ParseLevel(getenv("LOG_LEVEL"))
	if err == nil {
		stdLogger.SetLevel(level)
	}
}

type Logger interface {
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})

	Debug(args ...interface{})
	Info(args ...interface{})
	Print(args ...interface{})
	Warn(args ...interface{})

	Debugln(args ...interface{})
	Infoln(args ...interface{})
	Println(args ...interface{})
	Warnln(args ...interface{})
}

func SetLogger(l *logrus.Logger) {
	stdLogger = l
}

type clientLogger struct {
	Client    string
	ExecuteID string
	Op        Instruction
	Args      []Instruction
}

func (l clientLogger) WithClient(id string) clientLogger {
	l.Client = id
	return l
}

func (l clientLogger) WithExecuteID(id string) clientLogger {
	l.ExecuteID = id
	return l
}

func (l clientLogger) WithExecute(op Instruction, args []Instruction) clientLogger {
	l.Op = op
	l.Args = args
	return l
}

func (l clientLogger) Logger() Logger {
	fields := logrus.Fields{"channel": "client", "client": l.Client}
	if l.ExecuteID != "" {
		fields["channel"] = "execute"
		fields["executeId"] = l.ExecuteID
		if l.Op.ID.IsValid() {
			fields["op"] = l.Op.ID.NonceString()
		}
		if argc := len(l.Args); argc > 0 {
			fields["argc"] = argc
		}
		for i, v := range l.Args {
			k := fmt.Sprintf("arg%d", i)
			fields[k] = v.ID.NonceString()
		}
	}
	return stdLogger.WithFields(fields)
}

type serverLogger struct {
	Server       string
	Executer     string
	ExecutorHost string
	ExecuteID    string
	Op           Instruction
	Args         []Instruction
}

func (l serverLogger) WithServer(id string) serverLogger {
	l.Server = id
	return l
}

func (l serverLogger) WithExecutor(id string, host Host) serverLogger {
	l.Executer = id
	l.ExecutorHost = string(host)
	return l
}

func (l serverLogger) WithExecuteID(id string) serverLogger {
	l.ExecuteID = id
	return l
}

func (l serverLogger) WithExecute(op Instruction, args []Instruction) serverLogger {
	l.Op = op
	l.Args = args
	return l
}

func (l serverLogger) Logger() Logger {
	fields := logrus.Fields{"channel": "server", "server": l.Server}
	if l.Executer != "" {
		fields["channel"] = "execute"
		fields["executor"] = l.Executer
		fields["executorHost"] = l.ExecutorHost
		if l.ExecuteID != "" {
			fields["executeId"] = l.ExecuteID
		}
		if l.Op.ID.IsValid() {
			fields["op"] = l.Op.ID.NonceString()
		}
		if argc := len(l.Args); argc > 0 {
			fields["argc"] = argc
		}
		for i, v := range l.Args {
			k := fmt.Sprintf("arg%d", i)
			fields[k] = v.ID.NonceString()
		}
	}
	return stdLogger.WithFields(fields)
}
