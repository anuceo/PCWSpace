.PHONY: all julia elixir ui cli

all: julia elixir ui cli

julia:
	cd apps/qma-julia && julia --project=. -e 'using Pkg; Pkg.instantiate()'

elixir:
	cd apps/garden && mix deps.get

ui:
	cd apps/maiden-ui && npm install

cli:
	cd apps/maiden-cli && npm install && npm run build

dev-elixir:
	cd apps/garden && mix phx.server

dev-ui:
	cd apps/maiden-ui && npm run dev
