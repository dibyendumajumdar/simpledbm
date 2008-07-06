# Create the LaTeX output for SimpleDBM documents
# Documents refer to a number of images that must be in the images directory.
# These images are generated using the GraphViz dot package.

PATH="/Library/Frameworks/Python.framework/Versions/Current/bin:${PATH}"
PATH=$PATH:/usr/local/graphviz-2.12/bin
PATH=$PATH:~/Applications/docutils/tools

export PATH

for d in *.dot
do
    output=`basename ${d} .dot`
    echo "dot -Tpng ${d} > images/${output}.png"
    dot -Tpng ${d} > images/${output}.png
done
