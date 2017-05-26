<!-- MarkdownTOC -->

- [Introduction](#introduction)
- [Features](#features)
- [Details](#details)
    - [xslaveof command](#xslaveof-command)
    - [psync2 support](#psync2-support)
    - [force full sync](#force-full-sync)
    - [slave read only mode can replicate all commands to it's slaves](#slave-read-only-mode-can-replicate-all-commands-to-its-slaves)

<!-- /MarkdownTOC -->



<a name="introduction"></a>
# Introduction
XRedis is [ctrip](http://www.ctrip.com/) redis branch. Ctrip is a leading provider of travel services including accommodation reservation, transportation ticketing, packaged tours and corporate travel management.

<a name="features"></a>
# Features
* all features of redis 3.0.7 are inherited.
* xslaveof command support
* psync2 support


<a name="details"></a>
# Details

<a name="xslaveof-command"></a>
## xslaveof command

Suppose that redis slave is connectted to redis master(`ip1 port1`), at the mean time command `slaveof ip2 port2` is sent to this slave. Then redis will do the following:

1. Slave try this full resynchronization at the cron time(one time per second by default)

`xslaveof ip port` is a promotion for `slaveof`:

1. Slave try this partial resynchronization as soon as possible

<a name="psync2-support"></a>
## psync2 support
Here is the document for [psync2](https://gist.github.com/antirez/ae068f95c0d084891305)
<a name="force-full-sync"></a>
## force full sync
    * 命令 `refullsync`
    force all slaves reconnect itself, and fullsync with slaves
<a name="slave-read-only-mode-can-replicate-all-commands-to-its-slaves"></a>
## slave read only mode can replicate all commands to it's slaves
    **WARN: dangerous, you have to know what you are doing when using this command**

    config set slave-replicate-all yes
    config set slave-replicate-all no












