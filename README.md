# shiny-dataviewedit

Download the R script above to somewhere.

If you have shiny already installed and setup, you can just run directly
shiny::runApp("dataeditview.R")

``` {R} 
# Install `pak` if not already installed
if (!requireNamespace("pak", quietly = TRUE)) {
  install.packages("pak")
}

# Ensure all required packages are installed using `pak`
required_packages <- c("shiny", "DT", "arrow", "dplyr", "jsonlite", "fst", "purrr")
pak::pak(required_packages)
```

