#!/usr/bin/env python3
"""PgCustom - Minimal PostgreSQL GUI Client"""

import sys
from PySide6.QtWidgets import QApplication
from PySide6.QtCore import Qt

def main():
    # Enable high DPI scaling
    QApplication.setHighDpiScaleFactorRoundingPolicy(
        Qt.HighDpiScaleFactorRoundingPolicy.PassThrough
    )
    
    app = QApplication(sys.argv)
    app.setApplicationName("PgKKSql")
    app.setStyle("Fusion")
    
    # Import here to speed up initial load
    from ui import MainWindow
    
    window = MainWindow()
    window.showMaximized()  # Open maximized on current screen
    
    sys.exit(app.exec())

if __name__ == "__main__":
    main()

