# Contributing to Web Scraper & Dataset Builder

Thank you for your interest in contributing to the Web Scraper & Dataset Builder! This document provides guidelines and information for contributors.

## 🚀 Getting Started

### Prerequisites
- Python 3.8 or higher
- Git
- A GitHub account

### Setting Up Development Environment

1. **Fork the repository** on GitHub
2. **Clone your fork** locally:
   ```bash
   git clone https://github.com/YOUR_USERNAME/web-scraper-dataset-builder.git
   cd web-scraper-dataset-builder
   ```

3. **Set up the development environment**:
   
   **Windows:**
   ```cmd
   install_and_run.bat
   ```
   
   **Unix/Linux/macOS:**
   ```bash
   chmod +x install_and_run.sh
   ./install_and_run.sh
   ```

4. **Run tests** to ensure everything works:
   ```bash
   python test_basic.py
   python test_comprehensive.py
   ```

## 🛠️ Development Guidelines

### Code Style
- Follow PEP 8 Python style guidelines
- Use meaningful variable and function names
- Add docstrings to all classes and functions
- Keep functions focused and small
- Use type hints where appropriate

### Testing
- Write tests for new features
- Ensure all existing tests pass
- Aim for good test coverage
- Test both success and error cases

### Documentation
- Update README.md if adding new features
- Add inline comments for complex logic
- Update docstrings when modifying functions
- Include examples in documentation

## 📝 How to Contribute

### Reporting Bugs
1. Check if the bug has already been reported in Issues
2. Create a new issue with:
   - Clear, descriptive title
   - Steps to reproduce
   - Expected vs actual behavior
   - System information (OS, Python version)
   - Error messages or logs

### Suggesting Features
1. Check existing issues and discussions
2. Create a new issue with:
   - Clear description of the feature
   - Use cases and benefits
   - Possible implementation approach

### Submitting Code Changes

1. **Create a new branch** for your feature:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes**:
   - Write clean, well-documented code
   - Add tests for new functionality
   - Update documentation as needed

3. **Test your changes**:
   ```bash
   python test_basic.py
   python test_comprehensive.py
   ```

4. **Commit your changes**:
   ```bash
   git add .
   git commit -m "Add: brief description of your changes"
   ```

5. **Push to your fork**:
   ```bash
   git push origin feature/your-feature-name
   ```

6. **Create a Pull Request**:
   - Go to the original repository on GitHub
   - Click "New Pull Request"
   - Select your branch
   - Fill out the PR template

### Pull Request Guidelines
- **Title**: Clear, descriptive title
- **Description**: Explain what changes you made and why
- **Testing**: Describe how you tested your changes
- **Screenshots**: Include screenshots for UI changes
- **Breaking Changes**: Clearly mark any breaking changes

## 🏗️ Project Structure

```
web-scraper-dataset-builder/
├── main.py                 # Application entry point
├── config.py              # Configuration management
├── models.py              # Data models and structures
├── scraper.py             # Web scraping functionality
├── cleaner.py             # Data cleaning operations
├── export_manager.py      # Data export functionality
├── project_manager.py     # Project management
├── ui.py                  # User interface
├── utils/                 # Utility modules
│   ├── logger.py         # Logging functionality
│   └── error_handler.py  # Error handling
├── tests/                 # Test files
├── requirements.txt       # Python dependencies
└── README.md             # Project documentation
```

## 🧪 Testing

### Running Tests
```bash
# Basic functionality tests
python test_basic.py

# Comprehensive test suite
python test_comprehensive.py

# Advanced cleaning tests
python test_advanced_cleaning.py

# Scraping functionality tests
python test_scraping.py
```

### Writing Tests
- Place test files in the `tests/` directory
- Use descriptive test names
- Test both positive and negative cases
- Mock external dependencies when appropriate

## 📋 Areas for Contribution

### High Priority
- [ ] Additional data cleaning algorithms
- [ ] More export formats (PDF, XML, etc.)
- [ ] Enhanced error handling and recovery
- [ ] Performance optimizations
- [ ] Mobile-responsive UI improvements

### Medium Priority
- [ ] Plugin system for custom scrapers
- [ ] Database integration
- [ ] Scheduled scraping
- [ ] Data visualization features
- [ ] API endpoints

### Low Priority
- [ ] Internationalization (i18n)
- [ ] Theme customization
- [ ] Advanced filtering options
- [ ] Batch processing improvements

## 🤝 Community Guidelines

### Be Respectful
- Use welcoming and inclusive language
- Respect different viewpoints and experiences
- Accept constructive criticism gracefully
- Focus on what's best for the community

### Be Collaborative
- Help others learn and grow
- Share knowledge and resources
- Provide constructive feedback
- Celebrate others' contributions

## 📞 Getting Help

- **Issues**: Create a GitHub issue for bugs or feature requests
- **Discussions**: Use GitHub Discussions for questions and ideas
- **Documentation**: Check the README.md and inline documentation

## 🏆 Recognition

Contributors will be recognized in:
- README.md contributors section
- Release notes for significant contributions
- Special thanks in documentation

## 📄 License

By contributing to this project, you agree that your contributions will be licensed under the MIT License.

---

Thank you for contributing to Web Scraper & Dataset Builder! 🎉