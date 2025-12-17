"""PgKKSql UI Components"""

from typing import Optional, Any
from PySide6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QSplitter,
    QTreeWidget, QTreeWidgetItem, QPlainTextEdit, QTableView,
    QPushButton, QDialog, QFormLayout, QLineEdit, QSpinBox,
    QComboBox, QStatusBar, QToolBar, QMessageBox, QHeaderView,
    QStyledItemDelegate, QCheckBox, QDoubleSpinBox, QCompleter,
    QListView, QStackedWidget, QTextEdit
)
from PySide6.QtCore import Qt, QThread, Signal, QAbstractTableModel, QModelIndex, QStringListModel, QRect
from PySide6.QtGui import QFont, QAction, QKeySequence, QTextCursor

from db import Database, ConnectionInfo, load_connections, save_connections, get_last_connection, update_connection_timestamp


class SqlEditor(QPlainTextEdit):
    """SQL editor with autocomplete support."""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        
        # Completion data
        self._tables: list[str] = []  # Full table names: schema.table
        self._table_names: list[str] = []  # Just table names
        self._columns: list[str] = []
        self._keywords = [
            "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "LIKE", "ILIKE",
            "ORDER BY", "GROUP BY", "HAVING", "LIMIT", "OFFSET", "JOIN", "LEFT JOIN",
            "RIGHT JOIN", "INNER JOIN", "OUTER JOIN", "ON", "AS", "DISTINCT",
            "INSERT INTO", "VALUES", "UPDATE", "SET", "DELETE FROM", "CREATE TABLE",
            "ALTER TABLE", "DROP TABLE", "NULL", "IS NULL", "IS NOT NULL",
            "ASC", "DESC", "COUNT", "SUM", "AVG", "MIN", "MAX", "BETWEEN", "CASE",
            "WHEN", "THEN", "ELSE", "END", "COALESCE", "CAST", "TRUE", "FALSE"
        ]
        
        # Setup completer
        self._completer = QCompleter(self)
        self._completer.setWidget(self)
        self._completer.setCompletionMode(QCompleter.PopupCompletion)
        self._completer.setCaseSensitivity(Qt.CaseInsensitive)
        self._completer.setFilterMode(Qt.MatchContains)
        self._completer.activated.connect(self._insert_completion)
        
        # Model for completions
        self._model = QStringListModel()
        self._completer.setModel(self._model)
        
        # Style the popup
        popup = self._completer.popup()
        popup.setStyleSheet("""
            QListView {
                background-color: #2d2d2d;
                color: #e0e0e0;
                border: 1px solid #555;
                font-family: Consolas, monospace;
                font-size: 11pt;
            }
            QListView::item:selected {
                background-color: #0066cc;
            }
        """)
    
    def set_completions(self, tables: list[tuple[str, str]], columns: list[str]):
        """Update completion data from database."""
        self._tables = [f"{schema}.{table}" for schema, table in tables]
        self._table_names = [table for _, table in tables]
        self._columns = columns
    
    def _get_word_under_cursor(self) -> str:
        """Get the current word being typed."""
        cursor = self.textCursor()
        cursor.select(QTextCursor.WordUnderCursor)
        return cursor.selectedText()
    
    def _get_word_start_position(self) -> int:
        """Get position where current word starts."""
        cursor = self.textCursor()
        cursor.select(QTextCursor.WordUnderCursor)
        return cursor.selectionStart()
    
    def _insert_completion(self, completion: str):
        """Insert the selected completion."""
        cursor = self.textCursor()
        
        # Select and replace the current word
        cursor.select(QTextCursor.WordUnderCursor)
        cursor.removeSelectedText()
        cursor.insertText(completion)
        
        self.setTextCursor(cursor)
    
    def _update_completions(self):
        """Update and show completions based on current word."""
        word = self._get_word_under_cursor()
        
        if len(word) < 1:
            self._completer.popup().hide()
            return
        
        # Build completion list
        completions = []
        word_upper = word.upper()
        word_lower = word.lower()
        
        # Add matching keywords
        for kw in self._keywords:
            if word_upper in kw:
                completions.append(kw)
        
        # Add matching table names
        for table in self._table_names:
            if word_lower in table.lower():
                completions.append(table)
        
        # Add matching full table names (schema.table)
        for table in self._tables:
            if word_lower in table.lower():
                completions.append(table)
        
        # Add matching column names
        for col in self._columns:
            if word_lower in col.lower():
                completions.append(col)
        
        if not completions:
            self._completer.popup().hide()
            return
        
        # Remove duplicates, keep order
        seen = set()
        unique = []
        for c in completions:
            if c.lower() not in seen:
                seen.add(c.lower())
                unique.append(c)
        
        self._model.setStringList(unique[:20])  # Limit to 20 suggestions
        
        # Position popup
        cursor_rect = self.cursorRect()
        cursor_rect.setWidth(300)
        self._completer.complete(cursor_rect)
    
    def keyPressEvent(self, event):
        """Handle key presses for completion."""
        # If popup is visible
        if self._completer.popup().isVisible():
            if event.key() == Qt.Key_Tab:
                # Accept current completion
                index = self._completer.popup().currentIndex()
                if index.isValid():
                    self._completer.activated.emit(index.data())
                self._completer.popup().hide()
                return
            elif event.key() == Qt.Key_Escape:
                self._completer.popup().hide()
                return
            elif event.key() in (Qt.Key_Return, Qt.Key_Enter):
                # Accept completion if selected, otherwise normal enter
                index = self._completer.popup().currentIndex()
                if index.isValid():
                    self._completer.activated.emit(index.data())
                    self._completer.popup().hide()
                    return
            elif event.key() in (Qt.Key_Up, Qt.Key_Down):
                # Let completer handle navigation
                pass
        
        # Normal key handling
        super().keyPressEvent(event)
        
        # Update completions after typing
        if event.text() and event.text().isprintable():
            self._update_completions()
        elif event.key() == Qt.Key_Backspace:
            self._update_completions()


# PostgreSQL type OIDs for common types
PG_BOOL = 16
PG_INT2 = 21
PG_INT4 = 23
PG_INT8 = 20
PG_FLOAT4 = 700
PG_FLOAT8 = 701
PG_NUMERIC = 1700
PG_TEXT = 25
PG_VARCHAR = 1043
PG_CHAR = 18
PG_DATE = 1082
PG_TIMESTAMP = 1114
PG_TIMESTAMPTZ = 1184


class QueryWorker(QThread):
    """Worker thread for async query execution."""
    finished = Signal(list, list, list, str, int)  # rows, columns, types, error, rowcount
    
    def __init__(self, db: Database, query: str):
        super().__init__()
        self.db = db
        self.query = query
    
    def run(self):
        rows, columns, types, error, rowcount = self.db.execute_query(self.query)
        self.finished.emit(rows, columns, types, error or "", rowcount)


class ResultsModel(QAbstractTableModel):
    """Editable table model for query results."""
    
    editsChanged = Signal(int)  # Signal emitted when edit count changes
    
    def __init__(self):
        super().__init__()
        self._data: list[dict] = []
        self._columns: list[str] = []
        self._column_types: list[int] = []  # PostgreSQL type OIDs
        self._edits: dict[tuple[int, int], Any] = {}  # (row, col) -> new_value
        self._pk_columns: list[str] = []
        self._schema: str = ""
        self._table: str = ""
        self._is_error: bool = False
    
    def set_data(self, rows: list[dict], columns: list[str], column_types: list[int] = None):
        self.beginResetModel()
        self._data = rows
        self._columns = columns
        self._column_types = column_types or []
        self._edits.clear()
        self._is_error = len(columns) == 1 and columns[0] == "Error"
        self.endResetModel()
        self.editsChanged.emit(0)
    
    def set_table_info(self, schema: str, table: str, pk_columns: list[str]):
        self._schema = schema
        self._table = table
        self._pk_columns = pk_columns
    
    def clear(self):
        self.beginResetModel()
        self._data = []
        self._columns = []
        self._column_types = []
        self._edits.clear()
        self._is_error = False
        self.endResetModel()
        self.editsChanged.emit(0)
    
    def rowCount(self, parent=QModelIndex()):
        return len(self._data)
    
    def columnCount(self, parent=QModelIndex()):
        return len(self._columns)
    
    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if role == Qt.DisplayRole and orientation == Qt.Horizontal:
            if 0 <= section < len(self._columns):
                return self._columns[section]
        return None
    
    def get_column_type(self, col: int) -> int:
        """Get PostgreSQL type OID for column."""
        if 0 <= col < len(self._column_types):
            return self._column_types[col]
        return 0
    
    def data(self, index: QModelIndex, role=Qt.DisplayRole):
        if not index.isValid():
            return None
        
        row, col = index.row(), index.column()
        
        if role == Qt.DisplayRole:
            # Return edited value if exists, else original
            if (row, col) in self._edits:
                val = self._edits[(row, col)]
            elif 0 <= row < len(self._data) and 0 <= col < len(self._columns):
                val = self._data[row].get(self._columns[col])
            else:
                return None
            
            # Format boolean for display
            if isinstance(val, bool):
                return "✓" if val else "✗"
            return val
        
        elif role == Qt.EditRole:
            # Return raw value for editing
            if (row, col) in self._edits:
                return self._edits[(row, col)]
            if 0 <= row < len(self._data) and 0 <= col < len(self._columns):
                return self._data[row].get(self._columns[col])
        
        elif role == Qt.BackgroundRole:
            # Error rows get light red background
            if self._is_error:
                from PySide6.QtGui import QColor
                return QColor(255, 230, 230)  # Light red
            # Highlight edited cells with visible orange
            if (row, col) in self._edits:
                from PySide6.QtGui import QColor
                return QColor(255, 200, 150)  # Soft orange - clearly visible
        
        elif role == Qt.ForegroundRole:
            # Error rows get dark red text
            if self._is_error:
                from PySide6.QtGui import QColor
                return QColor(180, 0, 0)  # Dark red text
        
        elif role == Qt.TextAlignmentRole:
            # Center boolean values
            col_type = self.get_column_type(col)
            if col_type == PG_BOOL:
                return Qt.AlignCenter
        
        return None
    
    def setData(self, index: QModelIndex, value, role=Qt.EditRole):
        if role == Qt.EditRole and index.isValid():
            row, col = index.row(), index.column()
            original = self._data[row].get(self._columns[col])
            
            if value != original:
                self._edits[(row, col)] = value
            elif (row, col) in self._edits:
                del self._edits[(row, col)]
            
            self.dataChanged.emit(index, index)
            self.editsChanged.emit(len(self._edits))
            return True
        return False
    
    def flags(self, index: QModelIndex):
        flags = super().flags(index)
        # Only editable if we have table info with primary keys
        if self._table and self._pk_columns:
            flags |= Qt.ItemIsEditable
        return flags
    
    def get_pending_edits(self) -> list[tuple[str, list, str, Any]]:
        """Returns list of (pk_values, column, new_value) for pending edits."""
        edits = []
        for (row, col), new_value in self._edits.items():
            pk_values = [self._data[row].get(pk) for pk in self._pk_columns]
            column = self._columns[col]
            edits.append((pk_values, column, new_value))
        return edits
    
    def clear_edits(self):
        self._edits.clear()
        self.layoutChanged.emit()
        self.editsChanged.emit(0)
    
    @property
    def has_edits(self) -> bool:
        return bool(self._edits)
    
    @property
    def edit_count(self) -> int:
        return len(self._edits)
    
    @property
    def schema(self) -> str:
        return self._schema
    
    @property
    def table(self) -> str:
        return self._table
    
    @property
    def pk_columns(self) -> list[str]:
        return self._pk_columns


class TypeAwareDelegate(QStyledItemDelegate):
    """Custom delegate that provides type-appropriate editors."""
    
    def __init__(self, model: ResultsModel, parent=None):
        super().__init__(parent)
        self.model = model
    
    def createEditor(self, parent, option, index):
        col_type = self.model.get_column_type(index.column())
        
        if col_type == PG_BOOL:
            # Boolean: Use combobox with True/False/NULL
            editor = QComboBox(parent)
            editor.addItems(["True", "False", "NULL"])
            return editor
        
        elif col_type in (PG_INT2, PG_INT4, PG_INT8):
            # Integer: Use spinbox
            editor = QSpinBox(parent)
            editor.setRange(-2147483648, 2147483647)
            return editor
        
        elif col_type in (PG_FLOAT4, PG_FLOAT8, PG_NUMERIC):
            # Float: Use double spinbox
            editor = QDoubleSpinBox(parent)
            editor.setRange(-1e15, 1e15)
            editor.setDecimals(6)
            return editor
        
        else:
            # Default: Line edit for text
            editor = QLineEdit(parent)
            return editor
    
    def setEditorData(self, editor, index):
        value = index.data(Qt.EditRole)
        col_type = self.model.get_column_type(index.column())
        
        if col_type == PG_BOOL:
            if value is None:
                editor.setCurrentText("NULL")
            else:
                editor.setCurrentText("True" if value else "False")
        
        elif col_type in (PG_INT2, PG_INT4, PG_INT8):
            editor.setValue(int(value) if value is not None else 0)
        
        elif col_type in (PG_FLOAT4, PG_FLOAT8, PG_NUMERIC):
            editor.setValue(float(value) if value is not None else 0.0)
        
        else:
            editor.setText(str(value) if value is not None else "")
    
    def setModelData(self, editor, model, index):
        col_type = self.model.get_column_type(index.column())
        
        if col_type == PG_BOOL:
            text = editor.currentText()
            if text == "NULL":
                value = None
            else:
                value = text == "True"
        
        elif col_type in (PG_INT2, PG_INT4, PG_INT8):
            value = editor.value()
        
        elif col_type in (PG_FLOAT4, PG_FLOAT8, PG_NUMERIC):
            value = editor.value()
        
        else:
            value = editor.text()
            if value == "":
                value = None
        
        model.setData(index, value, Qt.EditRole)


class ConnectionDialog(QDialog):
    """Dialog for managing and selecting connections."""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Connect to Database")
        self.setMinimumWidth(400)
        
        self.connections = load_connections()
        self.selected_info: Optional[ConnectionInfo] = None
        
        self._setup_ui()
    
    def _setup_ui(self):
        layout = QVBoxLayout(self)
        
        # Saved connections dropdown
        conn_layout = QHBoxLayout()
        self.conn_combo = QComboBox()
        self.conn_combo.addItem("-- New Connection --")
        for c in self.connections:
            self.conn_combo.addItem(c.name)
        self.conn_combo.currentIndexChanged.connect(self._on_connection_selected)
        conn_layout.addWidget(self.conn_combo, 1)
        
        self.delete_btn = QPushButton("Delete")
        self.delete_btn.clicked.connect(self._delete_connection)
        self.delete_btn.setEnabled(False)
        conn_layout.addWidget(self.delete_btn)
        layout.addLayout(conn_layout)
        
        # Connection form
        form = QFormLayout()
        
        self.name_edit = QLineEdit()
        form.addRow("Name:", self.name_edit)
        
        self.host_edit = QLineEdit("localhost")
        form.addRow("Host:", self.host_edit)
        
        self.port_spin = QSpinBox()
        self.port_spin.setRange(1, 65535)
        self.port_spin.setValue(5432)
        form.addRow("Port:", self.port_spin)
        
        self.dbname_edit = QLineEdit("postgres")
        form.addRow("Database:", self.dbname_edit)
        
        self.user_edit = QLineEdit("postgres")
        form.addRow("User:", self.user_edit)
        
        self.password_edit = QLineEdit()
        self.password_edit.setEchoMode(QLineEdit.Password)
        form.addRow("Password:", self.password_edit)
        
        layout.addLayout(form)
        
        # Buttons
        btn_layout = QHBoxLayout()
        
        self.save_btn = QPushButton("Save")
        self.save_btn.clicked.connect(self._save_connection)
        btn_layout.addWidget(self.save_btn)
        
        btn_layout.addStretch()
        
        self.connect_btn = QPushButton("Connect")
        self.connect_btn.clicked.connect(self._connect)
        self.connect_btn.setDefault(True)
        btn_layout.addWidget(self.connect_btn)
        
        cancel_btn = QPushButton("Cancel")
        cancel_btn.clicked.connect(self.reject)
        btn_layout.addWidget(cancel_btn)
        
        layout.addLayout(btn_layout)
    
    def _on_connection_selected(self, index: int):
        self.delete_btn.setEnabled(index > 0)
        if index > 0:
            info = self.connections[index - 1]
            self.name_edit.setText(info.name)
            self.host_edit.setText(info.host)
            self.port_spin.setValue(info.port)
            self.dbname_edit.setText(info.dbname)
            self.user_edit.setText(info.user)
            self.password_edit.setText(info.password)
        else:
            self.name_edit.clear()
            self.host_edit.setText("localhost")
            self.port_spin.setValue(5432)
            self.dbname_edit.setText("postgres")
            self.user_edit.setText("postgres")
            self.password_edit.clear()
    
    def _get_current_info(self) -> ConnectionInfo:
        return ConnectionInfo(
            name=self.name_edit.text() or "Unnamed",
            host=self.host_edit.text(),
            port=self.port_spin.value(),
            dbname=self.dbname_edit.text(),
            user=self.user_edit.text(),
            password=self.password_edit.text()
        )
    
    def _save_connection(self):
        info = self._get_current_info()
        
        # Update existing or add new
        existing_idx = None
        for i, c in enumerate(self.connections):
            if c.name == info.name:
                existing_idx = i
                break
        
        if existing_idx is not None:
            self.connections[existing_idx] = info
        else:
            self.connections.append(info)
            self.conn_combo.addItem(info.name)
        
        save_connections(self.connections)
        
        # Select the saved connection
        idx = self.conn_combo.findText(info.name)
        if idx >= 0:
            self.conn_combo.setCurrentIndex(idx)
    
    def _delete_connection(self):
        idx = self.conn_combo.currentIndex()
        if idx > 0:
            del self.connections[idx - 1]
            save_connections(self.connections)
            self.conn_combo.removeItem(idx)
            self.conn_combo.setCurrentIndex(0)
    
    def _connect(self):
        self.selected_info = self._get_current_info()
        self.accept()


class MainWindow(QMainWindow):
    """Main application window."""
    
    def __init__(self):
        super().__init__()
        self.setWindowTitle("PgKKSql")
        self.resize(1200, 800)  # Fallback size
        
        self.db = Database()
        self.worker: Optional[QueryWorker] = None
        self.results_model = ResultsModel()
        self._original_info: Optional[ConnectionInfo] = None
        
        self._setup_ui()
        self._setup_toolbar()
        self._setup_statusbar()
        self._connect_signals()
        
        # Auto-connect to last used connection, or show dialog if none
        last_conn = get_last_connection()
        if last_conn:
            self._connect_to_db(last_conn)
        else:
            self._show_connect_dialog()
    
    def _setup_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        layout = QHBoxLayout(central)
        layout.setContentsMargins(4, 4, 4, 4)
        
        # Main splitter
        splitter = QSplitter(Qt.Horizontal)
        layout.addWidget(splitter)
        
        # Left sidebar - schema tree
        self.tree = QTreeWidget()
        self.tree.setHeaderLabel("Server")
        self.tree.setMinimumWidth(200)
        self.tree.itemExpanded.connect(self._on_tree_expand)
        self.tree.itemDoubleClicked.connect(self._on_tree_double_click)
        splitter.addWidget(self.tree)
        
        # Right side - editor and results
        right_splitter = QSplitter(Qt.Vertical)
        splitter.addWidget(right_splitter)
        
        # SQL editor with autocomplete
        self.editor = SqlEditor()
        self.editor.setPlaceholderText("Enter SQL query here...\nPress F5 or Ctrl+Enter to execute\nStart typing for autocomplete suggestions")
        font = QFont("Consolas", 11)
        font.setStyleHint(QFont.Monospace)
        self.editor.setFont(font)
        self.editor.setMinimumHeight(150)
        right_splitter.addWidget(self.editor)
        
        # Results area - stacked widget for table vs message
        self.results_stack = QStackedWidget()
        
        # Results table (index 0)
        self.results_table = QTableView()
        self.results_table.setModel(self.results_model)
        self.results_table.setAlternatingRowColors(True)
        self.results_table.horizontalHeader().setStretchLastSection(True)
        self.results_table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        
        # Set up type-aware delegate for editing
        self.delegate = TypeAwareDelegate(self.results_model, self.results_table)
        self.results_table.setItemDelegate(self.delegate)
        
        # Enable double-click editing
        self.results_table.setEditTriggers(QTableView.DoubleClicked | QTableView.EditKeyPressed)
        self.results_stack.addWidget(self.results_table)
        
        # Message area for errors/info (index 1)
        self.message_area = QTextEdit()
        self.message_area.setReadOnly(True)
        self.message_area.setFont(QFont("Consolas", 11))
        self.results_stack.addWidget(self.message_area)
        
        right_splitter.addWidget(self.results_stack)
        
        # Set splitter sizes
        splitter.setSizes([200, 1000])
        right_splitter.setSizes([300, 500])
    
    def _setup_toolbar(self):
        toolbar = QToolBar()
        toolbar.setMovable(False)
        self.addToolBar(toolbar)
        
        # Connect action
        connect_action = QAction("Connect", self)
        connect_action.triggered.connect(self._show_connect_dialog)
        toolbar.addAction(connect_action)
        
        toolbar.addSeparator()
        
        # Execute action
        execute_action = QAction("Execute (F5)", self)
        execute_action.setShortcuts([QKeySequence("F5"), QKeySequence("Ctrl+Return")])
        execute_action.triggered.connect(self._execute_query)
        toolbar.addAction(execute_action)
        
        toolbar.addSeparator()
        
        # Commit action
        self.commit_action = QAction("Commit", self)
        self.commit_action.triggered.connect(self._commit_changes)
        self.commit_action.setEnabled(False)
        toolbar.addAction(self.commit_action)
        
        # Rollback action
        self.rollback_action = QAction("Rollback", self)
        self.rollback_action.triggered.connect(self._rollback_changes)
        self.rollback_action.setEnabled(False)
        toolbar.addAction(self.rollback_action)
    
    def _setup_statusbar(self):
        self.statusbar = QStatusBar()
        self.setStatusBar(self.statusbar)
        self.statusbar.showMessage("Not connected")
    
    def _connect_signals(self):
        self.results_model.editsChanged.connect(self._on_edits_changed)
    
    def _show_connect_dialog(self):
        dialog = ConnectionDialog(self)
        if dialog.exec() == QDialog.Accepted and dialog.selected_info:
            self._connect_to_db(dialog.selected_info)
    
    def _connect_to_db(self, info: ConnectionInfo):
        try:
            self.db.connect(info)
            self.statusbar.showMessage(f"Connected to {info.name} ({info.host}:{info.port})")
            self.setWindowTitle(f"PgKKSql - {info.name}")
            update_connection_timestamp(info.name)  # Remember for next startup
            self._load_databases()
        except Exception as e:
            self.statusbar.showMessage(f"Connection failed: {e}")
            QMessageBox.critical(self, "Connection Error", str(e))
    
    def _load_databases(self):
        """Load all databases from the server."""
        self.tree.clear()
        if not self.db.is_connected():
            return
        
        # Store original connection for switching databases
        self._original_info = self.db.info
        
        # Update header to show connection name
        self.tree.setHeaderLabel(self.db.info.name)
        
        # Update autocomplete with tables and columns from current db
        self._update_completions()
        
        databases = self.db.get_databases()
        for dbname in databases:
            item = QTreeWidgetItem([dbname])
            item.setData(0, Qt.UserRole, ("database", dbname))
            # Add placeholder for lazy loading
            placeholder = QTreeWidgetItem(["Loading..."])
            item.addChild(placeholder)
            self.tree.addTopLevelItem(item)
    
    def _update_completions(self):
        """Update SQL editor autocomplete from current database."""
        if not self.db.is_connected():
            return
        tables = self.db.get_all_tables()
        columns = self.db.get_all_columns()
        self.editor.set_completions(tables, columns)
    
    def _on_tree_expand(self, item: QTreeWidgetItem):
        """Lazy load children when item is expanded."""
        data = item.data(0, Qt.UserRole)
        if not data:
            return
        
        # Check if already loaded
        if item.childCount() != 1 or item.child(0).text(0) != "Loading...":
            return
        
        item_type = data[0]
        
        if item_type == "database":
            # Load schemas for this database
            dbname = data[1]
            
            # Switch to the database to get its schemas
            self.db.switch_database(dbname)
            
            item.takeChildren()
            schemas = self.db.get_schemas()
            for schema in schemas:
                child = QTreeWidgetItem([schema])
                child.setData(0, Qt.UserRole, ("schema", dbname, schema))
                placeholder = QTreeWidgetItem(["Loading..."])
                child.addChild(placeholder)
                item.addChild(child)
            
            # Switch back to original database
            if self._original_info:
                self.db.connect(self._original_info)
        
        elif item_type == "schema":
            # Load tables for this schema
            dbname, schema = data[1], data[2]
            
            # Switch to the database to get its tables
            self.db.switch_database(dbname)
            
            item.takeChildren()
            tables = self.db.get_tables(schema)
            for table in tables:
                child = QTreeWidgetItem([table])
                child.setData(0, Qt.UserRole, ("table", dbname, schema, table))
                item.addChild(child)
            
            # Switch back to original database
            if self._original_info:
                self.db.connect(self._original_info)
    
    def _check_uncommitted_changes(self) -> bool:
        """Check for uncommitted changes and prompt user. Returns True if OK to proceed."""
        if self.results_model.has_edits:
            reply = QMessageBox.warning(
                self, "Uncommitted Changes",
                f"You have {self.results_model.edit_count} uncommitted cell edit(s).\n\n"
                "Opening a new table will discard these changes.\n\n"
                "Do you want to continue?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )
            if reply == QMessageBox.No:
                return False
            # Rollback changes
            self.db.rollback()
            self.results_model.clear_edits()
            self.commit_action.setEnabled(False)
            self.rollback_action.setEnabled(False)
        return True
    
    def _on_tree_double_click(self, item: QTreeWidgetItem, column: int):
        """Generate SELECT query on double-click."""
        data = item.data(0, Qt.UserRole)
        if not data or data[0] != "table":
            return
        
        # Check for uncommitted changes first
        if not self._check_uncommitted_changes():
            return
        
        dbname, schema, table = data[1], data[2], data[3]
        
        # Switch to the target database
        self.db.switch_database(dbname)
        self._original_info = self.db.info  # Update original to current
        self.setWindowTitle(f"PgKKSql - {self.db.info.name} ({dbname})")
        self.statusbar.showMessage(f"Switched to database: {dbname}")
        
        # Update autocomplete for new database
        self._update_completions()
        
        # Get primary key for ORDER BY
        pk_columns = self.db.get_primary_keys(schema, table)
        
        query = f'SELECT * FROM "{schema}"."{table}"'
        if pk_columns:
            order_cols = ", ".join(f'"{pk}"' for pk in pk_columns)
            query += f" ORDER BY {order_cols} ASC"
        
        self.editor.setPlainText(query)
        self._execute_query(schema=schema, table=table)
    
    def _parse_table_from_query(self, query: str) -> tuple[str, str]:
        """
        Try to extract schema and table from a simple SELECT query.
        Returns (schema, table) or ("", "") if unable to parse.
        Only works for single-table SELECTs without JOINs.
        """
        import re
        
        query_upper = query.upper()
        
        # Skip if it has JOINs - too complex for editing
        if " JOIN " in query_upper:
            return "", ""
        
        # Try to match: FROM "schema"."table" or FROM schema.table or FROM "table" or FROM table
        patterns = [
            r'FROM\s+"([^"]+)"\s*\.\s*"([^"]+)"',  # FROM "schema"."table"
            r'FROM\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\.\s*([a-zA-Z_][a-zA-Z0-9_]*)',  # FROM schema.table
            r'FROM\s+"([^"]+)"(?:\s|$|WHERE|ORDER|GROUP|LIMIT)',  # FROM "table"
            r'FROM\s+([a-zA-Z_][a-zA-Z0-9_]*)(?:\s|$|WHERE|ORDER|GROUP|LIMIT)',  # FROM table
        ]
        
        for i, pattern in enumerate(patterns):
            match = re.search(pattern, query, re.IGNORECASE)
            if match:
                if i < 2:
                    # Schema.table patterns
                    return match.group(1), match.group(2)
                else:
                    # Just table - assume public schema
                    return "public", match.group(1)
        
        return "", ""
    
    def _execute_query(self, schema: str = "", table: str = ""):
        if not self.db.is_connected():
            self.statusbar.showMessage("Not connected")
            return
        
        if self.worker and self.worker.isRunning():
            self.statusbar.showMessage("Query already running...")
            return
        
        query = self.editor.toPlainText().strip()
        if not query:
            self.statusbar.showMessage("No query to execute")
            return
        
        # Check for uncommitted changes before running a new SELECT
        if query.upper().startswith("SELECT") and self.results_model.has_edits:
            if not self._check_uncommitted_changes():
                return
        
        # If no table info provided, try to parse from query
        if not table and query.upper().startswith("SELECT"):
            schema, table = self._parse_table_from_query(query)
        
        # Store table info for editing
        self._pending_schema = schema
        self._pending_table = table
        
        self.statusbar.showMessage("Executing...")
        self.worker = QueryWorker(self.db, query)
        self.worker.finished.connect(self._on_query_finished)
        self.worker.start()
    
    def _on_query_finished(self, rows: list, columns: list, column_types: list, error: str, rowcount: int):
        if error:
            # Show error in message area
            self.message_area.setStyleSheet("""
                QTextEdit {
                    background-color: #fff0f0;
                    color: #cc0000;
                    border: 1px solid #ffcccc;
                    padding: 10px;
                }
            """)
            self.message_area.setText(f"❌ Query Error:\n\n{error}")
            self.results_stack.setCurrentIndex(1)  # Show message area
            self.statusbar.showMessage("Query failed - see error below")
            return
        
        if columns:
            # SELECT query - show results table
            self.results_stack.setCurrentIndex(0)  # Show table
            self.results_model.set_data(rows, columns, column_types)
            
            # Set table info if we have it
            schema = getattr(self, '_pending_schema', '')
            table = getattr(self, '_pending_table', '')
            
            if schema and table:
                pk_columns = self.db.get_primary_keys(schema, table)
                self.results_model.set_table_info(schema, table, pk_columns)
                # Store row count for status updates
                self._last_rowcount = rowcount
                editable = bool(pk_columns)
            else:
                self.results_model.set_table_info("", "", [])
                pk_columns = []
                editable = False
            
            msg = f"{rowcount} row{'s' if rowcount != 1 else ''}"
            if rowcount == 1000:
                msg += " (limited)"
            
            if editable:
                msg += f" • {schema}.{table} • Double-click to edit"
            elif schema and table and not pk_columns:
                msg += f" • {schema}.{table} • Read-only (no primary key)"
            else:
                # Check if it's a JOIN query
                query = self.editor.toPlainText().upper()
                if " JOIN " in query:
                    msg += " • Read-only (JOINs not editable)"
                else:
                    msg += " • Read-only (table not detected)"
            
            self.statusbar.showMessage(msg)
            
            # Resize columns to content
            self.results_table.resizeColumnsToContents()
        else:
            # DML query (UPDATE/INSERT/DELETE) - show success message
            self.message_area.setStyleSheet("""
                QTextEdit {
                    background-color: #f0fff0;
                    color: #006600;
                    border: 1px solid #ccffcc;
                    padding: 10px;
                }
            """)
            self.message_area.setText(f"✅ Query executed successfully\n\n{rowcount} row{'s' if rowcount != 1 else ''} affected\n\nClick 'Commit' to save changes or 'Rollback' to discard.")
            self.results_stack.setCurrentIndex(1)  # Show message area
            
            self._pending_dml_changes = rowcount
            msg = f"{rowcount} row{'s' if rowcount != 1 else ''} affected (uncommitted)"
            self.statusbar.showMessage(msg)
            # Enable commit/rollback buttons
            self.commit_action.setEnabled(True)
            self.rollback_action.setEnabled(True)
    
    def _on_edits_changed(self, edit_count: int):
        """Update UI when cell edits change."""
        has_edits = edit_count > 0
        self.commit_action.setEnabled(has_edits)
        self.rollback_action.setEnabled(has_edits)
        
        # Update status bar to show edit count
        if has_edits:
            rowcount = getattr(self, '_last_rowcount', 0)
            msg = f"{rowcount} row{'s' if rowcount != 1 else ''}"
            msg += f" • {edit_count} cell{'s' if edit_count != 1 else ''} edited (uncommitted)"
            self.statusbar.showMessage(msg)
        elif self.results_model.rowCount() > 0:
            rowcount = getattr(self, '_last_rowcount', self.results_model.rowCount())
            msg = f"{rowcount} row{'s' if rowcount != 1 else ''}"
            if self.results_model.table:
                msg += " • Double-click to edit"
            self.statusbar.showMessage(msg)
    
    def _commit_changes(self):
        # Handle cell edits in result grid
        if self.results_model.has_edits:
            schema = self.results_model.schema
            table = self.results_model.table
            pk_columns = self.results_model.pk_columns
            
            if not pk_columns:
                QMessageBox.warning(self, "Cannot Update", 
                                  "No primary key detected. Updates not supported for this query.")
                return
            
            errors = []
            edits = self.results_model.get_pending_edits()
            
            for pk_values, column, new_value in edits:
                error = self.db.execute_update(schema, table, pk_columns, pk_values, column, new_value)
                if error:
                    errors.append(error)
            
            if errors:
                self.db.rollback()
                self.statusbar.showMessage(f"Update failed: {errors[0]}")
                QMessageBox.warning(self, "Update Failed", "\n".join(errors))
                return
            
            self.results_model.clear_edits()
        
        # Commit transaction (for both cell edits and DML queries)
        self.db.commit()
        self.commit_action.setEnabled(False)
        self.rollback_action.setEnabled(False)
        self.statusbar.showMessage("Changes committed")
    
    def _rollback_changes(self):
        self.db.rollback()
        self.results_model.clear_edits()
        self.commit_action.setEnabled(False)
        self.rollback_action.setEnabled(False)
        self.statusbar.showMessage("Changes rolled back")
    
    def closeEvent(self, event):
        if self.results_model.has_edits:
            reply = QMessageBox.question(
                self, "Unsaved Changes",
                "You have uncommitted changes. Discard them?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )
            if reply == QMessageBox.No:
                event.ignore()
                return
        
        self.db.disconnect()
        event.accept()

