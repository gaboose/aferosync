package aferosync

type options struct {
	withSymlinks  bool
	withHardLinks bool
	withOwnership bool
}

type Option func(opts *options)

var defaultOpts = []Option{
	WithSymlinks(true),
	WithHardLinks(true),
	WithOwnership(true),
}

func WithSymlinks(v bool) Option {
	return func(opts *options) {
		opts.withSymlinks = v
	}
}

func WithHardLinks(v bool) Option {
	return func(opts *options) {
		opts.withHardLinks = v
	}
}

func WithOwnership(v bool) Option {
	return func(opts *options) {
		opts.withOwnership = v
	}
}
