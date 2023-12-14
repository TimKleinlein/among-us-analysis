#!/bin/bash

# Iterate over directories in the current directory
for originalDir in */; do
    # Skip if not a directory
    [ -d "$originalDir" ] || continue

    # Create a new directory in each originalDir
    newDir="$originalDir/srt"
    mkdir -p "$newDir"

    # Copy .srt files to new directory
    cp "$originalDir"/*.srt "$newDir/"
done
