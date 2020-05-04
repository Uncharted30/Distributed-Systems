package main

import (
	"fmt"
	"sync"
)

type Fetcher interface {
	// Fetch returns the body of URL and
	// a slice of URLs found on that page.
	Fetch(url string) (body string, urls []string, err error)
}

type emptyStruct struct{}
var void emptyStruct

type Crawler struct {
	crawled map[string]emptyStruct
	mu sync.Mutex
}

var crawler Crawler

// Crawl uses fetcher to recursively crawl
// pages starting with url, to a maximum of depth.
func Crawl(url string, depth int, fetcher Fetcher, waitGroup *sync.WaitGroup) {
	// TODO: Fetch URLs in parallel.
	// TODO: Don't fetch the same URL twice.
	// This implementation doesn't do either:

	defer waitGroup.Done()

	if depth <= 0 {
		return
	}

	crawler.mu.Lock()

	_, exists := crawler.crawled[url]

	if exists {
		crawler.mu.Unlock()
		return
	}
	crawler.crawled[url] = void
	crawler.mu.Unlock()

	body, urls, err := fetcher.Fetch(url)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("found: %s %q\n", url, body)

	currentWaitGroup := sync.WaitGroup{}

	for _, u := range urls {
		currentWaitGroup.Add(1)
		go Crawl(u, depth-1, fetcher, &currentWaitGroup)
	}

	currentWaitGroup.Wait()
	return
}

func main() {
	crawler.crawled = make(map[string]emptyStruct)
	waitGroup := sync.WaitGroup{}
	waitGroup.Add(1)
	Crawl("https://golang.org/", 4, fetcher, &waitGroup)
	waitGroup.Wait()
}

// fakeFetcher is Fetcher that returns canned results.
type fakeFetcher map[string]*fakeResult

type fakeResult struct {
	body string
	urls []string
}

func (f fakeFetcher) Fetch(url string) (string, []string, error) {
	if res, ok := f[url]; ok {
		return res.body, res.urls, nil
	}
	return "", nil, fmt.Errorf("not found: %s", url)
}

// fetcher is a populated fakeFetcher.
var fetcher = fakeFetcher{
	"https://golang.org/": &fakeResult{
		"The Go Programming Language",
		[]string{
			"https://golang.org/pkg/",
			"https://golang.org/cmd/",
		},
	},
	"https://golang.org/pkg/": &fakeResult{
		"Packages",
		[]string{
			"https://golang.org/",
			"https://golang.org/cmd/",
			"https://golang.org/pkg/fmt/",
			"https://golang.org/pkg/os/",
		},
	},
	"https://golang.org/pkg/fmt/": &fakeResult{
		"Package fmt",
		[]string{
			"https://golang.org/",
			"https://golang.org/pkg/",
		},
	},
	"https://golang.org/pkg/os/": &fakeResult{
		"Package os",
		[]string{
			"https://golang.org/",
			"https://golang.org/pkg/",
		},
	},
}

