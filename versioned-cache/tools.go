// +build tools

package versioned_cache

import (
	_ "github.com/golang/mock/mockgen"
	_ "github.com/launchdarkly/go-options"
)

/*
  This file is to keep go modules from removing tools
  needed by go generate scripts. The approach described in
  https://github.com/golang/go/wiki/Modules#how-can-i-track-tool-dependencies-for-a-module
*/
