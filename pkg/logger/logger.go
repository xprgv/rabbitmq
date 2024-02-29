package logger

type LogFunc func(msg string, args ...any)

type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

var _ Logger = (*Null)(nil)

type Null struct{}

func (Null) Debug(_ string, _ ...any) {}
func (Null) Info(_ string, _ ...any)  {}
func (Null) Warn(_ string, _ ...any)  {}
func (Null) Error(_ string, _ ...any) {}
