# Create the LaTeX output for SimpleDBM documents
# Documents refer to a number of images that must be in the images directory.
# These images are generated using the GraphViz dot package.

export PATH=$PATH:~/Applications/docutils/tools

rst2latex.py --documentclass="article" --documentoptions="oneside" --use-latex-footnotes --use-latex-citations --use-latex-abstract --use-verbatim-when-possible --table-style="booktabs" --use-latex-docinfo --use-latex-toc btree-space-management.rst btree-space-management.tex

rst2latex.py --documentclass="book" --documentoptions="oneside" --use-latex-footnotes --use-latex-citations --use-latex-abstract --use-verbatim-when-possible --table-style="standard" --use-latex-docinfo --use-latex-toc usermanual.rst usermanual.tex

rst2latex.py --documentclass="book" --documentoptions="onside" --use-latex-footnotes --use-latex-citations --use-latex-abstract --use-verbatim-when-possible --table-style="standard" --use-latex-docinfo --use-latex-toc developerguide.rst developerguide.tex

rst2latex.py --documentclass="book" --documentoptions="onside" --use-latex-footnotes --use-latex-citations --use-latex-abstract --use-verbatim-when-possible --table-style="standard" --use-latex-docinfo --use-latex-toc typesystem.rst typesystem.tex

rst2latex.py --documentclass="book" --documentoptions="onside" --use-latex-footnotes --use-latex-citations --use-latex-abstract --use-verbatim-when-possible --table-style="standard" --use-latex-docinfo --use-latex-toc database-api.rst database-api.tex
