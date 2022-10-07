#!/bin/bash -x

bundle exec ruby generate.rb > report.txt
bbedit report.txt