#!/bin/bash
for f in /etc/profile.d/*.sh; do source $f; done
$1
