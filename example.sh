#!/bin/bash
#sbt clean compile
~/Personal/Projects/asciinema-stdin/humanize-cat.py "./example.txt" | asciinema rec -i "./example.cast"
