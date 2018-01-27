#!/usr/bin/env bash

set -e

s3_path=s3://<YOUR_PREFIX>/dsdt/dsdt_test/

red=`tput setaf 1`
green=`tput setaf 2`
reset=`tput sgr0`

dothing () {
echo -n "${reset}${1}${reset}"
echo -n "${red}"
read -n 1
${1} || true
echo "${reset}"
}

# echo "${green}Disdat Demo One${reset}"
dsdt context -d disdat.demo.otheruser > /dev/null

dsdt switch disdat.demo > /dev/null

dothing "dsdt remote --force disdat.demo.share $s3_path"

dothing "dsdt push -b average"

dothing "dsdt context disdat.demo.otheruser"

dothing "dsdt switch disdat.demo.otheruser"

dothing "dsdt remote --force disdat.demo.share $s3_path"

dothing "dsdt ls"

dothing "dsdt pull -b average"

dothing "dsdt cat average"
