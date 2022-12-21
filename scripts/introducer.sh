#!/bin/bash
mp1_dir=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
cd "$mp1_dir"

java -cp ../target/mp4_idunno-1.0-SNAPSHOT-jar-with-dependencies.jar com.cs425.membership.IntroducerServer $1