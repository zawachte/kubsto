package loader

import "context"

type Loader interface {
	Load(context.Context) error
}
