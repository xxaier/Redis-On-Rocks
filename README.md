<!-- MarkdownTOC -->

1. [Introduction](#introduction)
1. [Features](#features)
1. [Details](#details)
    1. [xslaveof command](#xslaveof-command)

<!-- /MarkdownTOC -->



<a name="introduction"></a>
# Introduction
XRedis is [ctrip](http://www.ctrip.com/) redis branch. Ctrip is a leading provider of travel services including accommodation reservation, transportation ticketing, packaged tours and corporate travel management.

<a name="features"></a>
# Features
* all features of redis 3.0.7 are inherited.
* xslaveof command support


<a name="details"></a>
# Details

<a name="xslaveof-command"></a>
## xslaveof command

Suppose that redis slave is connectted to redis master(`ip1 port1`), at the mean time command `slaveof ip2 port2` is sent to this slave. Then redis will do the following:

1. Slave will try a full resynchronization  
2. Slave try this full resynchronization at the cron time(one time per second by default)

`xslaveof ip port` is a promotion for `slaveof`:

1. Slave will try a partial resynchronization
2. Slave try this partial resynchronization as soon as possible