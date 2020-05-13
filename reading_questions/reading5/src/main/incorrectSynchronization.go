import "sync"

var a string
var done bool
var l sync.Mutex

// because of l.Lock() in setup, setting values of a and done are atomic
// after done is set to true and a is set to "hello, world",
// l.Unlock() in setup() happens before l.Lock() in the for loop
// so the intended result is guaranteed
func setup() {
	l.Lock()
	a = "hello, world"
	done = true
	l.Unlock()
}

func main() {
	go setup()
	for {
		l.Lock()
		if done {
			break
		}
		l.Unlock()
	}
	l.Unlock()
	print(a)
}