# Install `pak` if not already installed
if (!requireNamespace("pak", quietly = TRUE)) {
  install.packages("pak")
}

# Ensure all required packages are installed using `pak`
required_packages <- c("shiny", "DT", "arrow", "dplyr", "jsonlite", "fst", "purrr")
pak::pak(required_packages)

library(shiny)
library(DT)
library(arrow)
library(dplyr)
library(jsonlite)
library(fst)

# Set max file size to system memory or 500 MB by default
max_memory <- as.numeric(system("awk '/MemTotal/ {print $2}' /proc/meminfo", intern = TRUE)) * 1024
if (is.na(max_memory)) max_memory <- 500 * 1024^2
options(shiny.maxRequestSize = max_memory)

ui <- fluidPage(
  titlePanel("Enhanced File Viewer and Editor"),
  sidebarLayout(
    sidebarPanel(
      fileInput(
        "file",
        "Upload File",
        accept = c(".parquet", ".feather", ".fst", ".csv", ".json"),
        multiple = FALSE
      ),
      helpText(paste0("Maximum file size: ", round(max_memory / 1024^2), " MB.")),
      numericInput("page_size", "Rows per Page", value = 100, min = 10, step = 10),
      numericInput("start_row", "Start Row", value = 1, min = 1, step = 10),
      selectInput(
        "save_format",
        "Save Format",
        choices = c("Parquet" = "parquet", "Feather" = "feather", "FST" = "fst", "CSV" = "csv", "JSON" = "json"),
        selected = "parquet"
      ),
      downloadButton("download", "Download Edited File")
    ),
    mainPanel(
      DTOutput("table")
    )
  )
)

server <- function(input, output, session) {
  dataset <- reactiveVal(NULL)  # Reactive value to store the dataset
  edits <- reactiveValues(data = list())  # Store staged edits
  dataset_format <- reactiveVal(NULL)  # Track file format for proper handling

  # Load the file and initialize dataset
  observeEvent(input$file, {
    req(input$file)
    ext <- tools::file_ext(input$file$name)

    # Load dataset based on file type
    ds <- switch(ext,
      "parquet" = {
        showNotification("Using chunked reading for Parquet files.", type = "message")
        dataset_format("parquet")
        arrow::open_dataset(input$file$datapath, format = "parquet")
      },
      "feather" = {
        showNotification("Using chunked reading for Feather files.", type = "message")
        dataset_format("feather")
        arrow::open_dataset(input$file$datapath, format = "feather")
      },
      "fst" = {
        showNotification("FST files are loaded into memory. Consider converting to Parquet for better performance.", type = "warning")
        dataset_format("fst")
        fst::read_fst(input$file$datapath)
      },
      "csv" = {
        showNotification("CSV files are loaded into memory. Consider converting to Parquet for better performance.", type = "warning")
        dataset_format("csv")
        read.csv(input$file$datapath)
      },
      "json" = {
        showNotification("JSON files are loaded into memory. Consider converting to Parquet for better performance.", type = "warning")
        dataset_format("json")
        as_tibble(jsonlite::fromJSON(input$file$datapath))
      },
      stop("Unsupported file type!")
    )

    # Assign dataset based on type
    if (inherits(ds, "Dataset")) {
      dataset(ds)  # For Parquet and Feather
    } else {
      dataset(as_tibble(ds))  # For FST, CSV, and JSON
    }
  })

  # Paginate and fetch rows
  paginated_data <- reactive({
    req(dataset())
    page_size <- input$page_size
    start_row <- input$start_row

    if (inherits(dataset(), "Dataset")) {
      # Handle chunked reading for Parquet/Feather using head/tail
      dataset() %>%
        slice_head(n = start_row + page_size - 1) %>%
        slice_tail(n = page_size) %>%
        collect()
    } else {
      # Handle in-memory data for FST/CSV/JSON
      dataset() %>%
        slice(start_row:(start_row + page_size - 1))
    }
  })

  # Render the DataTable
  output$table <- renderDT({
    req(paginated_data())
    datatable(
      paginated_data(),
      editable = "cell",
      options = list(
        pageLength = input$page_size,
        dom = "Bfrtip",
        searching = TRUE
      )
    )
  })

  # Handle cell edits
  observeEvent(input$table_cell_edit, {
    info <- input$table_cell_edit
    row <- input$start_row + info$row - 1  # Adjust for pagination
    edits$data[[row]] <- list(column = info$col, value = info$value)
  })

  # Save the edited file
  output$download <- downloadHandler(
    filename = function() {
      paste0("edited_", Sys.Date(), ".", input$save_format)
    },
    content = function(file) {
      req(dataset())
      
      # Collect the dataset into a data frame
      df <- if (inherits(dataset(), "Dataset")) {
        dataset() %>% collect()
      } else {
        dataset()
      }
      
      # Apply edits to the dataset
      if (length(edits$data) > 0) {
        for (row in names(edits$data)) {
          edit <- edits$data[[row]]
          df[as.numeric(row), as.numeric(edit$column)] <- edit$value
        }
      }

      # Save the dataset in the selected format
      switch(input$save_format,
        "parquet" = arrow::write_parquet(df, file),
        "feather" = arrow::write_feather(df, file),
        "fst" = fst::write_fst(df, file),
        "csv" = write.csv(df, file, row.names = FALSE),
        "json" = jsonlite::write_json(df, file, pretty = TRUE),
        stop("Unsupported save format!")
      )
    }
  )
}

shinyApp(ui, server)