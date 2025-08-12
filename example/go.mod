module github.com/jehiah/gomrjob/example

go 1.23.0

replace github.com/jehiah/gomrjob => ../

require (
	github.com/jehiah/gomrjob v0.0.0-00010101000000-000000000000
	github.com/jehiah/lru v1.0.0
)

require (
	cloud.google.com/go/compute/metadata v0.5.0 // indirect
	github.com/bitly/go-simplejson v0.5.1 // indirect
	golang.org/x/oauth2 v0.30.0 // indirect
	golang.org/x/sys v0.35.0 // indirect
)
