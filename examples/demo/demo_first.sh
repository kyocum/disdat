#!/usr/bin/env bash

set -e

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

echo "${green}Disdat Demo One${reset}"

dsdt context -d disdat.demo > /dev/null

dothing "dsdt context disdat.demo"

dothing "dsdt switch disdat.demo"

dothing "dsdt apply - average demo_pipeline.Average"

dothing "dsdt ls -v -i"

dothing "dsdt cat GenData"

dothing "dsdt cat average"

dothing "dsdt commit average"

dothing "dsdt ls -v average"