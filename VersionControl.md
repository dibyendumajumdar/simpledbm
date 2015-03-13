# Introduction #

The SimpleDBM source code has been migrated from subversion to mercurial. This page documents a few commonly used commands.

# Publishing changes #

To check whether there are any outgoing changes:
```
hg outgoing -p
```

To push the changes into the central repository:
```
hg push https://simpledbm.googlecode.com/hg
```

# Checking out SimpleDBM #

You can checkout the SimpleDBM source code by executing:
```
hg clone http://simpledbm.googlecode.com/hg simpledbm
```