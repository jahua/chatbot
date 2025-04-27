# Tourism Data Chatbot

An intelligent chatbot system for analyzing tourism data in Switzerland, powered by OpenAI.

## Features

- Interactive chat interface built with Streamlit
- Backend API powered by FastAPI
- Data analysis and visualization capabilities
- Support for querying tourism statistics
- Real-time data visualization with Plotly
- Session management and chat history
- Fast dependency management with uv

## Tech Stack

- Python 3.11+
- FastAPI
- Streamlit
- PostgreSQL
- OpenAI
- Plotly
- Pandas
- uv (fast Python package installer)

## Setup

### Quick Setup (with uv)

1. Clone the repository:
```bash
git clone https://github.com/jahua/chatbot.git
cd chatbot
```

2. Run the setup script:
```bash
python setup_env.py
```

3. Activate the virtual environment:
```bash
source venv311/bin/activate  # On Windows: venv311\Scripts\activate
```

4. Run the setup script again (inside the virtual environment):
```bash
python setup_env.py
```

5. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your configuration
```

### Traditional Setup (without uv)

1. Clone the repository:
```bash
git clone https://github.com/jahua/chatbot.git
cd chatbot
```

2. Create and activate virtual environment:
```bash
python -m venv venv311
source venv311/bin/activate  # On Windows: venv311\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your configuration
```

## Running the Application

### Easy Start (Single Command)

To start both the backend and frontend with a single command:

```bash
./start.sh
```

### Manual Start

1. Start the backend server:
```bash
./run_server.py
# or
python run_server.py
```

2. Start the frontend application:
```bash
./run_frontend.py
# or
python run_frontend.py
```

The application will be available at:
- Frontend: http://localhost:8501
- Backend API: http://localhost:8080

## Using uv for Dependency Management

This project uses [uv](https://github.com/astral-sh/uv), a fast Python package installer and resolver. Benefits include:

- Up to 10-100x faster package installations
- Reliable dependency resolution
- Optimized for CI/CD pipelines
- Compatible with pip and requirements.txt

To use uv manually:

```bash
# Install a package
uv pip install package-name

# Install from requirements.txt
uv pip install -r requirements.txt

# Create a virtual environment
uv venv venv311
```
# Run backend server with uv
uv run uvicorn app.main:app

# Run frontend with uv
uv run streamlit run app.py

## Project Structure

```
chatbot/
├── app/                    # Backend application
│   ├── core/              # Core functionality
│   ├── db/                # Database models and connection
│   ├── llm/              # LLM integration
│   ├── models/           # Data models
│   ├── schemas/          # Pydantic schemas
│   └── services/         # Business logic
├── frontend/             # Streamlit frontend
├── tests/               # Test suite
├── .env                 # Environment variables
├── pyproject.toml      # Project configuration
├── requirements.txt     # Python dependencies
├── setup_env.py        # Environment setup script
├── run_server.py       # Backend server runner
├── run_frontend.py     # Frontend runner
├── start.sh            # Complete startup script
└── README.md           # Project documentation
```

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

# Academic Paper Converter (Markdown to Word)

This tool converts Markdown-formatted academic papers to properly formatted Microsoft Word documents, following standard academic styling guidelines.

## Features

- Proper formatting of headings according to academic standards
- Title case and sentence case handling where appropriate
- Paragraph indentation and double spacing
- Page numbers in footers
- Running headers with shortened paper title
- Support for lists (ordered and unordered)
- Special handling for Abstract section

## Installation

1. Clone this repository:
```
git clone https://github.com/yourusername/academic-paper-converter.git
cd academic-paper-converter
```

2. Install the required dependencies:
```
pip install python-docx markdown bs4
```

## Usage

### Basic Usage

To convert your Markdown paper to Word, simply run:

```
python convert_to_word.py
```

By default, the script looks for a file named `note/paper/academic-paper.md` and outputs to `academic-paper.docx` in the current directory.

### Custom Input/Output Files

To specify custom input and output files, modify the script variables in the `if __name__ == "__main__"` section:

```python
input_file = "path/to/your/paper.md"
output_file = "path/to/desired/output.docx"
```

## Formatting Guidelines

The converter applies the following formatting rules:

1. **Level 1 Headings (# in Markdown)**:
   - Centered, bold, title case

2. **Level 2 Headings (## in Markdown)**:
   - Left-aligned, bold, title case

3. **Level 3 Headings (### in Markdown)**:
   - Indented, bold, title case
   - Ends with a period
   - Paragraph text begins on the same line

4. **Level 4 Headings (#### in Markdown)**:
   - Indented, bold italic, sentence case
   - Ends with a period
   - Paragraph text begins on the same line

5. **Regular Paragraphs**:
   - Indented first line (0.5 inches)
   - Double-spaced
   - Times New Roman, 12pt font

6. **Lists**:
   - Proper bullet points for unordered lists
   - Numbered for ordered lists

## Markdown Best Practices

For best results with this converter:

1. Start your paper with a level 1 heading (`#`) as the title
2. Include an "Abstract" section (using a level 1 heading)
3. Use heading levels consistently and hierarchically
4. Use standard Markdown syntax for lists:
   ```
   - Item 1
   - Item 2
   
   1. First item
   2. Second item
   ```

## Troubleshooting

If you encounter issues:

- Ensure your Markdown is properly formatted
- Check that you have the latest versions of the dependencies
- Verify file paths and permissions

## License

This project is licensed under the MIT License - see the LICENSE file for details.

# Markdown to Word Converter

A Python tool for converting Markdown files to Microsoft Word (.docx) documents with high fidelity formatting.

## Features

- Converts Markdown to well-formatted Word documents
- Supports a wide range of Markdown features:
  - Headings (H1-H6)
  - Paragraphs with formatting
  - Bold and italic text
  - Ordered and unordered lists
  - Tables
  - Code blocks with monospaced fonts
  - Images (local with optional image directory)
  - Blockquotes
  - Links
- Optional Word template support
- Batch processing mode
- Command-line interface for easy use

## Installation

1. Clone this repository:
   ```
   git clone https://github.com/username/md2word-converter.git
   cd md2word-converter
   ```

2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

## Usage

### Basic Usage

Convert a single Markdown file to Word:

```bash
python convert.py example.md
```

This will create `example.docx` in the same directory.

### Advanced Options

Specify an output file:

```bash
python convert.py example.md -o output.docx
```

Use a Word template:

```bash
python convert.py example.md -t template.docx
```

Specify a directory for images:

```bash
python convert.py example.md -i ./images
```

Batch process all Markdown files in a directory:

```bash
python convert.py ./markdown_folder -b
```

Process all Markdown files and save results to a specific directory:

```bash
python convert.py ./markdown_folder -b -o ./output_folder
```

### Help

Display help information:

```bash
python convert.py --help
```

## Using as a Library

You can also use the converter in your own Python code:

```python
from md2word import MarkdownToWord

# Initialize the converter
converter = MarkdownToWord(img_dir='./images', template='template.docx')

# Convert a file
converter.convert('input.md', 'output.docx')
```

## Supported Markdown Features

### Headings
```
# Heading 1
## Heading 2
### Heading 3
```

### Text Formatting
```
**Bold text**
*Italic text*
`inline code`
```

### Lists
```
* Unordered item 1
* Unordered item 2

1. Ordered item 1
2. Ordered item 2
```

### Tables
```
| Header 1 | Header 2 |
|----------|----------|
| Cell 1   | Cell 2   |
```

### Code Blocks
```
​```python
def hello():
    print("Hello, World!")
​```
```

### Blockquotes
```
> This is a blockquote
```

### Links
```
[Link text](https://www.example.com)
```

### Images
```
![Alt text](image.jpg)
```

## Example

See the included `example.md` file and convert it to see the results:

```bash
python convert.py example.md
```

## Requirements

- Python 3.7+
- Dependencies:
  - python-docx
  - markdown
  - beautifulsoup4
  - Pillow
  - click

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Limitations

- Nested lists support is limited
- Remote images need to be downloaded separately
- Some advanced styling may not be perfectly preserved

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
