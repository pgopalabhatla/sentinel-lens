#!/usr/bin/env bash
# Re-render the README diagrams (overview, architecture, router) from their .mmd files to .svg and a 2x .png.
set -e
cd "$(dirname "$0")"

MMDC="npx -y @mermaid-js/mermaid-cli@11.4.2"

render() {  # name, config, page width for the png (wide enough that the 2x png is not scaled down)
    $MMDC -i "$1.mmd" -o "$1.svg" -c "$2" -b white
    $MMDC -i "$1.mmd" -o "$1.png" -c "$2" -b white -w "$3" -s 2
}

render overview     mermaid-config.json        800
render architecture mermaid-config.json        800
render router       mermaid-config-router.json 1160
