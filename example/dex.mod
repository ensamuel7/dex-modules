module github.com/ensamuel7/dex-modules/example

require (
    github.com/ensamuel7/dex-modules/sqlite v0.1.0
    github.com/ensamuel7/dex-modules/redis v0.1.0
)

replace (
    github.com/ensamuel7/dex-modules/sqlite => ../sqlite
    github.com/ensamuel7/dex-modules/redis => ../redis
)
