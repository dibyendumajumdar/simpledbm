# Introduction #

SimpleDBM documents are created in reStructuredText. This is useful as it is low maintenance - most documentation is just plain text. But at the same time, it is possible to generate LaTeX PDF output, and/or HTML output.

# Pre-requisites #

The pre-requisites are:

  * Latest version of [Python](http://www.python.org).
  * The [docutils](http://docutils.sourceforge.net/index.html) package.
  * The [GraphViz](http://www.graphviz.org/) package.

# Generate LaTeX documents #

Use following commands to generate the SimpleDBM LaTeX outputs

```
     sh genlatex.sh
```

The genlatex.sh script is in the docs directory. The generated files (.tex
extension) will be saved in the docs directory.

# Generate the diagrams #

Some of the diagrams referenced in SimpleDBM documents are created using the
dot language. These diagrams are saved as files with .dot extension in the
docs directory. To create the required image files, use following command:

```
     sh genimages.sh
```

The diagrams will be generated and saved in the docs/images directory.