// Streamline Web UI - JavaScript

// Toast notification system
const toastContainer = document.createElement('div');
toastContainer.className = 'toast-container';
document.body.appendChild(toastContainer);

function showToast(message, type = 'info', duration = 5000) {
    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    toast.innerHTML = `
        <span>${message}</span>
        <button class="toast-close" onclick="this.parentElement.remove()">&times;</button>
    `;
    toastContainer.appendChild(toast);

    if (duration > 0) {
        setTimeout(() => toast.remove(), duration);
    }
}

// Utility functions
function formatBytes(bytes) {
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    let i = 0;
    while (bytes >= 1024 && i < units.length - 1) {
        bytes /= 1024;
        i++;
    }
    return `${bytes.toFixed(2)} ${units[i]}`;
}

function formatNumber(n) {
    if (n >= 1e9) return `${(n / 1e9).toFixed(2)}B`;
    if (n >= 1e6) return `${(n / 1e6).toFixed(2)}M`;
    if (n >= 1e3) return `${(n / 1e3).toFixed(2)}K`;
    return n.toString();
}

function timeAgo(timestamp) {
    const seconds = Math.floor((Date.now() - timestamp * 1000) / 1000);

    if (seconds < 60) return 'just now';
    if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
    if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
    return `${Math.floor(seconds / 86400)}d ago`;
}

// Table sorting
function sortTable(table, column, direction = 'asc') {
    const tbody = table.querySelector('tbody');
    const rows = Array.from(tbody.querySelectorAll('tr'));

    rows.sort((a, b) => {
        const aVal = a.cells[column].textContent.trim();
        const bVal = b.cells[column].textContent.trim();

        // Try numeric comparison first
        const aNum = parseFloat(aVal);
        const bNum = parseFloat(bVal);

        if (!isNaN(aNum) && !isNaN(bNum)) {
            return direction === 'asc' ? aNum - bNum : bNum - aNum;
        }

        // Fall back to string comparison
        return direction === 'asc'
            ? aVal.localeCompare(bVal)
            : bVal.localeCompare(aVal);
    });

    rows.forEach(row => tbody.appendChild(row));
}

// Add sorting to tables
document.querySelectorAll('.data-table th').forEach((th, index) => {
    th.style.cursor = 'pointer';
    th.addEventListener('click', () => {
        const table = th.closest('table');
        const currentDir = th.dataset.sortDir || 'asc';
        const newDir = currentDir === 'asc' ? 'desc' : 'asc';

        // Reset all headers
        table.querySelectorAll('th').forEach(h => {
            h.dataset.sortDir = '';
            h.textContent = h.textContent.replace(/[▲▼]/, '').trim();
        });

        // Update clicked header
        th.dataset.sortDir = newDir;
        th.textContent += newDir === 'asc' ? ' ▲' : ' ▼';

        sortTable(table, index, newDir);
    });
});

// Table search/filter
function filterTable(input, tableId) {
    const filter = input.value.toLowerCase();
    const table = document.getElementById(tableId);
    if (!table) return;

    const rows = table.querySelectorAll('tbody tr');

    rows.forEach(row => {
        const text = row.textContent.toLowerCase();
        row.style.display = text.includes(filter) ? '' : 'none';
    });
}

// Command Palette
class CommandPalette {
    constructor() {
        this.modal = document.getElementById('command-palette');
        this.input = document.getElementById('command-palette-input');
        this.results = document.getElementById('command-palette-results');
        this.selectedIndex = -1;
        this.searchResults = [];
        this.recentCommands = this.loadRecentCommands();
        this.debounceTimer = null;
        this.isOpen = false;

        if (this.modal && this.input) {
            this.setupEventListeners();
        }
    }

    setupEventListeners() {
        // Input events
        this.input.addEventListener('input', () => this.onInput());
        this.input.addEventListener('keydown', (e) => this.onKeyDown(e));

        // Close on overlay click
        this.modal.addEventListener('click', (e) => {
            if (e.target === this.modal) {
                this.close();
            }
        });
    }

    open() {
        if (!this.modal) return;
        this.modal.classList.remove('hidden');
        this.input.value = '';
        this.input.focus();
        this.selectedIndex = -1;
        this.isOpen = true;
        // Show recent commands or empty state
        this.showRecentCommands();
    }

    close() {
        if (!this.modal) return;
        this.modal.classList.add('hidden');
        this.input.value = '';
        this.searchResults = [];
        this.selectedIndex = -1;
        this.isOpen = false;
    }

    toggle() {
        if (this.isOpen) {
            this.close();
        } else {
            this.open();
        }
    }

    onInput() {
        const query = this.input.value.trim();

        // Debounce search
        clearTimeout(this.debounceTimer);
        this.debounceTimer = setTimeout(() => {
            if (query.length === 0) {
                this.showRecentCommands();
            } else {
                this.search(query);
            }
        }, 150);
    }

    async search(query) {
        try {
            const response = await fetch(`/api/search?q=${encodeURIComponent(query)}`);
            if (!response.ok) throw new Error('Search failed');
            const data = await response.json();
            this.searchResults = data.results;
            this.renderResults();
        } catch (error) {
            console.error('Search error:', error);
            this.results.innerHTML = '<div class="command-palette-empty">Search failed</div>';
        }
    }

    showRecentCommands() {
        if (this.recentCommands.length === 0) {
            this.results.innerHTML = `
                <div class="command-palette-empty">
                    <span class="muted">Type to search topics, groups, or actions...</span>
                </div>
            `;
            return;
        }

        this.searchResults = this.recentCommands;
        this.results.innerHTML = `
            <div class="command-palette-section">Recent</div>
            ${this.recentCommands.map((item, index) => this.renderResultItem(item, index)).join('')}
        `;
        this.selectedIndex = -1;
    }

    renderResults() {
        if (this.searchResults.length === 0) {
            this.results.innerHTML = '<div class="command-palette-empty">No results found</div>';
            return;
        }

        // Group results by type
        const groups = {
            action: [],
            topic: [],
            consumergroup: [],
            broker: []
        };

        this.searchResults.forEach(item => {
            const type = item.result_type.toLowerCase();
            if (groups[type]) {
                groups[type].push(item);
            }
        });

        let html = '';
        let globalIndex = 0;

        const typeLabels = {
            action: 'Actions',
            topic: 'Topics',
            consumergroup: 'Consumer Groups',
            broker: 'Brokers'
        };

        for (const [type, items] of Object.entries(groups)) {
            if (items.length > 0) {
                html += `<div class="command-palette-section">${typeLabels[type]}</div>`;
                items.forEach(item => {
                    html += this.renderResultItem(item, globalIndex);
                    globalIndex++;
                });
            }
        }

        this.results.innerHTML = html;
        this.selectedIndex = -1;
    }

    renderResultItem(item, index) {
        return `
            <div class="command-palette-item ${index === this.selectedIndex ? 'selected' : ''}"
                 data-index="${index}"
                 data-url="${item.url}"
                 onclick="window.commandPalette.selectItem(${index})">
                <span class="command-palette-icon">${this.getIcon(item.icon)}</span>
                <div class="command-palette-item-content">
                    <span class="command-palette-item-name">${this.escapeHtml(item.name)}</span>
                    <span class="command-palette-item-desc">${this.escapeHtml(item.description)}</span>
                </div>
                <span class="command-palette-type">${item.result_type}</span>
            </div>
        `;
    }

    getIcon(iconName) {
        const icons = {
            'database': '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/></svg>',
            'users': '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg>',
            'server': '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="2" y="2" width="20" height="8" rx="2" ry="2"/><rect x="2" y="14" width="20" height="8" rx="2" ry="2"/><line x1="6" y1="6" x2="6.01" y2="6"/><line x1="6" y1="18" x2="6.01" y2="18"/></svg>',
            'plus': '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>',
            'chart': '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="18" y1="20" x2="18" y2="10"/><line x1="12" y1="20" x2="12" y2="4"/><line x1="6" y1="20" x2="6" y2="14"/></svg>',
            'grid': '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/></svg>',
            'file-text': '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/></svg>',
            'bell': '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.73 21a2 2 0 0 1-3.46 0"/></svg>',
            'zap': '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg>',
            'link': '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71"/><path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71"/></svg>'
        };
        return icons[iconName] || icons['database'];
    }

    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    onKeyDown(e) {
        const itemCount = this.searchResults.length;

        switch (e.key) {
            case 'ArrowDown':
                e.preventDefault();
                if (itemCount > 0) {
                    this.selectedIndex = (this.selectedIndex + 1) % itemCount;
                    this.updateSelection();
                }
                break;

            case 'ArrowUp':
                e.preventDefault();
                if (itemCount > 0) {
                    this.selectedIndex = this.selectedIndex <= 0 ? itemCount - 1 : this.selectedIndex - 1;
                    this.updateSelection();
                }
                break;

            case 'Enter':
                e.preventDefault();
                if (this.selectedIndex >= 0 && this.selectedIndex < itemCount) {
                    this.selectItem(this.selectedIndex);
                }
                break;

            case 'Escape':
                e.preventDefault();
                this.close();
                break;
        }
    }

    updateSelection() {
        const items = this.results.querySelectorAll('.command-palette-item');
        items.forEach((item, index) => {
            item.classList.toggle('selected', index === this.selectedIndex);
        });

        // Scroll into view
        const selectedItem = items[this.selectedIndex];
        if (selectedItem) {
            selectedItem.scrollIntoView({ block: 'nearest' });
        }
    }

    selectItem(index) {
        const item = this.searchResults[index];
        if (!item) return;

        // Save to recent commands
        this.saveRecentCommand(item);

        // Navigate to the URL
        this.close();
        window.location.href = item.url;
    }

    loadRecentCommands() {
        try {
            const stored = localStorage.getItem('streamline-recent-commands');
            return stored ? JSON.parse(stored) : [];
        } catch {
            return [];
        }
    }

    saveRecentCommand(item) {
        // Remove duplicates
        this.recentCommands = this.recentCommands.filter(c => c.url !== item.url);
        // Add to front
        this.recentCommands.unshift(item);
        // Keep only last 10
        this.recentCommands = this.recentCommands.slice(0, 10);
        // Save
        try {
            localStorage.setItem('streamline-recent-commands', JSON.stringify(this.recentCommands));
        } catch {
            // Ignore storage errors
        }
    }
}

// Message Template Manager
class TemplateManager {
    constructor() {
        this.templates = this.loadTemplates();
        this.currentTopic = null;
    }

    loadTemplates() {
        try {
            const stored = localStorage.getItem('streamline-message-templates');
            return stored ? JSON.parse(stored) : [];
        } catch {
            return [];
        }
    }

    saveTemplates() {
        try {
            localStorage.setItem('streamline-message-templates', JSON.stringify(this.templates));
        } catch {
            // Ignore storage errors
        }
    }

    getTemplates(topic = null) {
        if (!topic) return this.templates;
        return this.templates.filter(t => !t.topic || t.topic === topic);
    }

    getTemplate(id) {
        return this.templates.find(t => t.id === id);
    }

    saveTemplate(template) {
        // Generate ID if new
        if (!template.id) {
            template.id = 'tpl-' + Date.now() + '-' + Math.random().toString(36).substr(2, 9);
            template.created_at = Date.now();
        }

        // Update or add
        const index = this.templates.findIndex(t => t.id === template.id);
        if (index >= 0) {
            this.templates[index] = template;
        } else {
            this.templates.push(template);
        }

        this.saveTemplates();
        return template;
    }

    deleteTemplate(id) {
        this.templates = this.templates.filter(t => t.id !== id);
        this.saveTemplates();
    }

    markUsed(id) {
        const template = this.getTemplate(id);
        if (template) {
            template.last_used_at = Date.now();
            this.saveTemplates();
        }
    }

    // Variable expansion
    expandVariables(text) {
        if (!text) return text;

        const now = new Date();
        const variables = {
            'timestamp': () => Date.now().toString(),
            'uuid': () => crypto.randomUUID ? crypto.randomUUID() : 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, c => {
                const r = Math.random() * 16 | 0;
                return (c === 'x' ? r : (r & 0x3 | 0x8)).toString(16);
            }),
            'date': () => now.toISOString().split('T')[0],
            'time': () => now.toTimeString().split(' ')[0],
            'datetime': () => now.toISOString(),
            'year': () => now.getFullYear().toString(),
            'month': () => String(now.getMonth() + 1).padStart(2, '0'),
            'day': () => String(now.getDate()).padStart(2, '0'),
            'hour': () => String(now.getHours()).padStart(2, '0'),
            'minute': () => String(now.getMinutes()).padStart(2, '0'),
            'second': () => String(now.getSeconds()).padStart(2, '0')
        };

        // Replace simple variables like {{timestamp}}
        let result = text.replace(/\{\{(\w+)\}\}/g, (match, varName) => {
            const fn = variables[varName.toLowerCase()];
            return fn ? fn() : match;
        });

        // Replace random:min-max
        result = result.replace(/\{\{random:(\d+)-(\d+)\}\}/g, (match, min, max) => {
            const minNum = parseInt(min, 10);
            const maxNum = parseInt(max, 10);
            return String(Math.floor(Math.random() * (maxNum - minNum + 1)) + minNum);
        });

        // Replace sequence:name (increments each time)
        result = result.replace(/\{\{sequence:(\w+)\}\}/g, (match, seqName) => {
            const key = `streamline-sequence-${seqName}`;
            let value = parseInt(localStorage.getItem(key) || '0', 10);
            value++;
            localStorage.setItem(key, value.toString());
            return value.toString();
        });

        return result;
    }

    // Export templates to JSON
    exportTemplates() {
        const data = JSON.stringify(this.templates, null, 2);
        const blob = new Blob([data], { type: 'application/json' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = 'streamline-templates.json';
        a.click();
        URL.revokeObjectURL(url);
    }

    // Import templates from JSON
    importTemplates(file) {
        return new Promise((resolve, reject) => {
            const reader = new FileReader();
            reader.onload = (e) => {
                try {
                    const imported = JSON.parse(e.target.result);
                    if (!Array.isArray(imported)) {
                        reject(new Error('Invalid template file format'));
                        return;
                    }
                    // Merge templates (don't overwrite existing)
                    for (const template of imported) {
                        if (!this.getTemplate(template.id)) {
                            template.id = 'tpl-' + Date.now() + '-' + Math.random().toString(36).substr(2, 9);
                            this.templates.push(template);
                        }
                    }
                    this.saveTemplates();
                    resolve(imported.length);
                } catch (err) {
                    reject(err);
                }
            };
            reader.onerror = () => reject(new Error('Failed to read file'));
            reader.readAsText(file);
        });
    }
}

// Initialize managers
let commandPalette;
let templateManager;
document.addEventListener('DOMContentLoaded', () => {
    commandPalette = new CommandPalette();
    window.commandPalette = commandPalette;

    templateManager = new TemplateManager();
    window.templateManager = templateManager;

    // Initialize template UI if on produce page
    initTemplateUI();
});

// Template UI functions
function initTemplateUI() {
    const produceForm = document.getElementById('produce-form');
    if (!produceForm) return;

    // Get current topic from form
    const formAction = produceForm.getAttribute('onsubmit');
    const topicMatch = formAction && formAction.match(/produceMessage\(event,\s*'([^']+)'\)/);
    const currentTopic = topicMatch ? topicMatch[1] : null;
    if (templateManager) {
        templateManager.currentTopic = currentTopic;
    }
}

// Apply a template to the produce form
function applyTemplate(templateId) {
    if (!templateManager) return;

    const template = templateManager.getTemplate(templateId);
    if (!template) return;

    // Fill in the form
    const keyInput = document.getElementById('key');
    const valueInput = document.getElementById('value');

    if (keyInput && template.key_template) {
        keyInput.value = templateManager.expandVariables(template.key_template);
    }
    if (valueInput) {
        valueInput.value = templateManager.expandVariables(template.value_template);
    }

    // Set headers
    const headersContainer = document.getElementById('headers-container');
    if (headersContainer && template.headers) {
        // Clear existing headers
        headersContainer.innerHTML = '';
        // Add template headers
        for (const [key, value] of Object.entries(template.headers)) {
            addHeaderRow();
            const rows = headersContainer.querySelectorAll('.header-row');
            const lastRow = rows[rows.length - 1];
            const inputs = lastRow.querySelectorAll('input');
            if (inputs[0]) inputs[0].value = templateManager.expandVariables(key);
            if (inputs[1]) inputs[1].value = templateManager.expandVariables(value);
        }
        if (Object.keys(template.headers).length === 0) {
            addHeaderRow(); // Add empty row
        }
    }

    templateManager.markUsed(templateId);
    StreamlineUI.showToast(`Applied template: ${template.name}`, 'success');
}

// Save current form as template
function saveAsTemplate() {
    const name = prompt('Template name:');
    if (!name) return;

    const keyInput = document.getElementById('key');
    const valueInput = document.getElementById('value');
    const headersContainer = document.getElementById('headers-container');

    // Gather headers
    const headers = {};
    if (headersContainer) {
        const rows = headersContainer.querySelectorAll('.header-row');
        rows.forEach(row => {
            const inputs = row.querySelectorAll('input');
            if (inputs[0] && inputs[1] && inputs[0].value) {
                headers[inputs[0].value] = inputs[1].value;
            }
        });
    }

    const template = {
        name: name,
        topic: templateManager.currentTopic || '',
        key_template: keyInput ? keyInput.value : null,
        value_template: valueInput ? valueInput.value : '',
        headers: headers,
        description: null
    };

    templateManager.saveTemplate(template);
    updateTemplateSelector();
    StreamlineUI.showToast(`Saved template: ${name}`, 'success');
}

// Delete a template
function deleteTemplate(templateId) {
    if (!confirm('Delete this template?')) return;
    if (!templateManager) return;

    const template = templateManager.getTemplate(templateId);
    templateManager.deleteTemplate(templateId);
    updateTemplateSelector();
    StreamlineUI.showToast(`Deleted template: ${template ? template.name : 'Unknown'}`, 'info');
}

// Update the template selector dropdown
function updateTemplateSelector(topic) {
    const selector = document.getElementById('template-selector');
    if (!selector) return;

    // Initialize template manager if not already done
    if (!window.templateManager) {
        window.templateManager = new TemplateManager();
    }

    // Set current topic if provided
    if (topic) {
        window.templateManager.currentTopic = topic;
    }

    const templates = window.templateManager.getTemplates(window.templateManager.currentTopic);

    let options = '<option value="">Load Template...</option>';
    templates.forEach(t => {
        const lastUsed = t.last_used_at ? ` (used ${StreamlineUI.timeAgo(t.last_used_at / 1000)})` : '';
        options += `<option value="${t.id}">${t.name}${lastUsed}</option>`;
    });

    selector.innerHTML = options;
}

// Show template manager modal
function showTemplateManager() {
    const modal = document.getElementById('template-manager-modal');
    if (modal) {
        modal.classList.remove('hidden');
        renderTemplateList();
    }
}

// Hide template manager modal
function hideTemplateManager() {
    const modal = document.getElementById('template-manager-modal');
    if (modal) {
        modal.classList.add('hidden');
    }
}

// Render the template list in the manager modal
function renderTemplateList() {
    const container = document.getElementById('template-list');
    if (!container || !window.templateManager) return;

    const templates = window.templateManager.getTemplates();

    if (templates.length === 0) {
        container.innerHTML = '<div class="template-empty">No templates saved yet. Create one by filling out the form and clicking "Save as Template".</div>';
        return;
    }

    container.innerHTML = templates.map(t => `
        <div class="template-item" data-id="${t.id}">
            <div class="template-item-info">
                <div class="template-item-name">${escapeHtml(t.name)}</div>
                <div class="template-item-topic">Topic: ${escapeHtml(t.topic)}${t.description ? ' - ' + escapeHtml(t.description) : ''}</div>
            </div>
            <div class="template-item-actions">
                <button type="button" class="btn btn-small" onclick="applyTemplateFromManager('${t.id}')">Apply</button>
                <button type="button" class="btn btn-small btn-danger" onclick="deleteTemplateFromManager('${t.id}')">Delete</button>
            </div>
        </div>
    `).join('');
}

// Apply template from the manager modal
function applyTemplateFromManager(templateId) {
    applyTemplate(templateId);
    hideTemplateManager();
}

// Delete template from the manager modal
function deleteTemplateFromManager(templateId) {
    if (confirm('Are you sure you want to delete this template?')) {
        deleteTemplate(templateId);
        renderTemplateList();
    }
}

// Helper to escape HTML to prevent XSS
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// Export templates to JSON file
function exportTemplates() {
    if (!window.templateManager) return;
    window.templateManager.exportTemplates();
}

// Import templates from JSON file
function importTemplates(event) {
    if (!window.templateManager) return;
    window.templateManager.importTemplates(event.target.files[0]).then(() => {
        renderTemplateList();
        updateTemplateSelector();
        showToast('Templates imported successfully', 'success');
    }).catch(err => {
        showToast('Failed to import templates: ' + err.message, 'error');
    });
}

// Vim-style Navigation
class VimNavigator {
    constructor() {
        this.enabled = localStorage.getItem('vimNavEnabled') !== 'false';
        this.selectedIndex = -1;
        this.currentList = null;
        this.init();
    }

    init() {
        // Find and track navigable lists on the page
        this.updateNavigableLists();

        // Re-scan when content changes
        const observer = new MutationObserver(() => this.updateNavigableLists());
        observer.observe(document.body, { childList: true, subtree: true });
    }

    updateNavigableLists() {
        const lists = document.querySelectorAll('.vim-navigable');
        if (lists.length > 0 && !this.currentList) {
            this.setActiveList(lists[0]);
        }
    }

    setActiveList(list) {
        // Remove selection from previous list
        if (this.currentList) {
            this.clearSelection();
        }
        this.currentList = list;
        this.selectedIndex = -1;
    }

    getItems() {
        if (!this.currentList) return [];
        return Array.from(this.currentList.querySelectorAll('.vim-item'));
    }

    clearSelection() {
        if (this.currentList) {
            this.currentList.querySelectorAll('.vim-selected').forEach(el => {
                el.classList.remove('vim-selected');
            });
        }
    }

    selectIndex(index) {
        const items = this.getItems();
        if (items.length === 0) return;

        // Clamp index
        if (index < 0) index = 0;
        if (index >= items.length) index = items.length - 1;

        this.clearSelection();
        this.selectedIndex = index;

        const item = items[index];
        item.classList.add('vim-selected');
        item.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
    }

    moveUp() {
        if (!this.enabled) return false;
        const items = this.getItems();
        if (items.length === 0) return false;

        if (this.selectedIndex <= 0) {
            this.selectIndex(0);
        } else {
            this.selectIndex(this.selectedIndex - 1);
        }
        return true;
    }

    moveDown() {
        if (!this.enabled) return false;
        const items = this.getItems();
        if (items.length === 0) return false;

        if (this.selectedIndex < 0) {
            this.selectIndex(0);
        } else {
            this.selectIndex(this.selectedIndex + 1);
        }
        return true;
    }

    goToTop() {
        if (!this.enabled) return false;
        const items = this.getItems();
        if (items.length === 0) return false;
        this.selectIndex(0);
        return true;
    }

    goToBottom() {
        if (!this.enabled) return false;
        const items = this.getItems();
        if (items.length === 0) return false;
        this.selectIndex(items.length - 1);
        return true;
    }

    selectCurrent() {
        if (!this.enabled) return false;
        const items = this.getItems();
        if (this.selectedIndex < 0 || this.selectedIndex >= items.length) return false;

        const item = items[this.selectedIndex];
        // Find link or clickable element within item
        const link = item.querySelector('a') || item.querySelector('[onclick]') || item;
        if (link.href) {
            window.location.href = link.href;
        } else if (link.onclick) {
            link.click();
        }
        return true;
    }

    toggle() {
        this.enabled = !this.enabled;
        localStorage.setItem('vimNavEnabled', this.enabled);
        if (!this.enabled) {
            this.clearSelection();
        }
        StreamlineUI.showToast(
            this.enabled ? 'Vim navigation enabled (j/k/g/G)' : 'Vim navigation disabled',
            'info'
        );
        return this.enabled;
    }

    isEnabled() {
        return this.enabled;
    }
}

// Initialize vim navigator
let vimNavigator = null;
document.addEventListener('DOMContentLoaded', () => {
    vimNavigator = new VimNavigator();
    window.vimNavigator = vimNavigator;
});

// Deep Linking Utility
class DeepLink {
    constructor() {
        this.params = {};
        this.parseHash();
        this.init();
    }

    init() {
        // Listen for hash changes
        window.addEventListener('hashchange', () => {
            this.parseHash();
            this.restoreState();
        });

        // Initial state restoration
        this.restoreState();
    }

    parseHash() {
        this.params = {};
        const hash = window.location.hash.substring(1);
        if (!hash) return;

        const pairs = hash.split('&');
        for (const pair of pairs) {
            const [key, value] = pair.split('=');
            if (key && value) {
                this.params[decodeURIComponent(key)] = decodeURIComponent(value);
            }
        }
    }

    updateHash(params) {
        const pairs = [];
        for (const [key, value] of Object.entries(params)) {
            if (value !== null && value !== undefined && value !== '') {
                pairs.push(`${encodeURIComponent(key)}=${encodeURIComponent(value)}`);
            }
        }
        window.location.hash = pairs.length > 0 ? pairs.join('&') : '';
    }

    getParam(key, defaultValue = null) {
        return this.params[key] ?? defaultValue;
    }

    setParam(key, value) {
        if (value === null || value === undefined || value === '') {
            delete this.params[key];
        } else {
            this.params[key] = value;
        }
        this.updateHash(this.params);
    }

    restoreState() {
        // Highlight message if specified
        const messageOffset = this.getParam('message');
        if (messageOffset) {
            this.highlightMessage(messageOffset);
        }

        // Restore search filter if specified
        const searchQuery = this.getParam('search');
        if (searchQuery) {
            const searchInput = document.querySelector('.search-input');
            if (searchInput) {
                searchInput.value = searchQuery;
                // Trigger filter
                const event = new Event('keyup', { bubbles: true });
                searchInput.dispatchEvent(event);
            }
        }
    }

    highlightMessage(offset) {
        // Find message card with matching offset
        const messageCards = document.querySelectorAll('.message-card');
        for (const card of messageCards) {
            const offsetSpan = card.querySelector('.message-offset');
            if (offsetSpan && offsetSpan.textContent.includes(`Offset: ${offset}`)) {
                // Remove previous highlights
                document.querySelectorAll('.message-highlighted').forEach(el => {
                    el.classList.remove('message-highlighted');
                });

                // Highlight and scroll to this message
                card.classList.add('message-highlighted');
                card.scrollIntoView({ behavior: 'smooth', block: 'center' });
                return;
            }
        }
    }

    // Generate a shareable URL for the current page state
    getShareableUrl(additionalParams = {}) {
        const url = new URL(window.location.href);
        const allParams = { ...this.params, ...additionalParams };

        const pairs = [];
        for (const [key, value] of Object.entries(allParams)) {
            if (value !== null && value !== undefined && value !== '') {
                pairs.push(`${encodeURIComponent(key)}=${encodeURIComponent(value)}`);
            }
        }

        url.hash = pairs.length > 0 ? pairs.join('&') : '';
        return url.toString();
    }
}

// Initialize deep link utility
let deepLink = null;
document.addEventListener('DOMContentLoaded', () => {
    deepLink = new DeepLink();
    window.deepLink = deepLink;
});

// Share button functionality
function shareLink(params = {}) {
    const url = deepLink ? deepLink.getShareableUrl(params) : window.location.href;
    copyToClipboard(url);
}

function shareMessage(offset) {
    shareLink({ message: offset });
}

function copyToClipboard(text) {
    if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(text).then(() => {
            StreamlineUI.showToast('Link copied to clipboard', 'success');
        }).catch(err => {
            fallbackCopyToClipboard(text);
        });
    } else {
        fallbackCopyToClipboard(text);
    }
}

function fallbackCopyToClipboard(text) {
    const textarea = document.createElement('textarea');
    textarea.value = text;
    textarea.style.position = 'fixed';
    textarea.style.left = '-9999px';
    document.body.appendChild(textarea);
    textarea.select();

    try {
        document.execCommand('copy');
        StreamlineUI.showToast('Link copied to clipboard', 'success');
    } catch (err) {
        StreamlineUI.showToast('Failed to copy link', 'error');
    }

    document.body.removeChild(textarea);
}

// Copy current page URL
function copyPageUrl() {
    copyToClipboard(window.location.href);
}

// Time Machine - Visual timeline scrubber for topic data
class TimeMachine {
    constructor() {
        this.data = null;
        this.currentPosition = 0; // 0-1 position on timeline
        this.isPlaying = false;
        this.playbackSpeed = 1;
        this.playbackInterval = null;
        this.topicName = null;
        this.partition = null;
        this.init();
    }

    init() {
        // Only initialize if time machine container exists
        const container = document.getElementById('time-machine');
        if (!container) return;

        // Extract topic name and partition from URL
        const pathMatch = window.location.pathname.match(/\/topics\/([^\/]+)\/browse/);
        if (pathMatch) {
            this.topicName = decodeURIComponent(pathMatch[1]);

            // Get partition from query string
            const params = new URLSearchParams(window.location.search);
            this.partition = params.get('partition');
        }

        // Set up timeline bar click handler
        const timelineBar = document.getElementById('timeline-bar');
        if (timelineBar) {
            timelineBar.addEventListener('click', (e) => this.handleTimelineClick(e));
            timelineBar.addEventListener('mousemove', (e) => this.handleTimelineHover(e));
        }

        // Load timeline data
        this.loadTimeline();
    }

    async loadTimeline() {
        if (!this.topicName) return;

        try {
            const url = `/api/topics/${encodeURIComponent(this.topicName)}/timeline` +
                (this.partition ? `?partition=${this.partition}` : '');

            const response = await fetch(url);
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }

            this.data = await response.json();
            this.renderTimeline();
        } catch (error) {
            console.error('Failed to load timeline:', error);
            this.showError('Failed to load timeline data');
        }
    }

    renderTimeline() {
        if (!this.data) return;

        // Update labels
        const startLabel = document.getElementById('timeline-start');
        const endLabel = document.getElementById('timeline-end');
        const currentLabel = document.getElementById('timeline-current');

        if (startLabel) {
            startLabel.textContent = this.formatTimestamp(this.data.start_timestamp_ms);
        }
        if (endLabel) {
            endLabel.textContent = this.formatTimestamp(this.data.end_timestamp_ms);
        }
        if (currentLabel) {
            currentLabel.textContent = this.formatTimestamp(
                this.interpolateTimestamp(this.currentPosition)
            );
        }

        // Update stats
        const totalMessages = document.getElementById('tm-total-messages');
        const timeRange = document.getElementById('tm-time-range');

        if (totalMessages) {
            totalMessages.textContent = this.data.total_messages.toLocaleString();
        }
        if (timeRange) {
            const duration = this.data.end_timestamp_ms - this.data.start_timestamp_ms;
            timeRange.textContent = this.formatDuration(duration);
        }

        // Render density visualization
        this.renderDensity();

        // Update cursor position
        this.updateCursor();
    }

    renderDensity() {
        const densityEl = document.getElementById('timeline-density');
        if (!densityEl || !this.data || !this.data.buckets.length) return;

        // Find max count for normalization
        const maxCount = Math.max(...this.data.buckets.map(b => b.count));
        if (maxCount === 0) return;

        // Create density bars
        const bars = this.data.buckets.map((bucket, i) => {
            const height = Math.round((bucket.count / maxCount) * 100);
            const width = 100 / this.data.buckets.length;
            return `<div class="density-bar" style="left: ${i * width}%; width: ${width}%; height: ${height}%;" title="${bucket.count} messages"></div>`;
        }).join('');

        densityEl.innerHTML = bars;
    }

    updateCursor() {
        const cursor = document.getElementById('timeline-cursor');
        const progress = document.getElementById('timeline-progress');
        const currentLabel = document.getElementById('timeline-current');

        if (cursor) {
            cursor.style.left = `${this.currentPosition * 100}%`;
        }
        if (progress) {
            progress.style.width = `${this.currentPosition * 100}%`;
        }
        if (currentLabel && this.data) {
            currentLabel.textContent = this.formatTimestamp(
                this.interpolateTimestamp(this.currentPosition)
            );
        }
    }

    handleTimelineClick(e) {
        const bar = e.currentTarget;
        const rect = bar.getBoundingClientRect();
        const position = Math.max(0, Math.min(1, (e.clientX - rect.left) / rect.width));

        this.currentPosition = position;
        this.updateCursor();
        this.seekToPosition(position);
    }

    handleTimelineHover(e) {
        if (!this.data) return;

        const bar = e.currentTarget;
        const rect = bar.getBoundingClientRect();
        const position = Math.max(0, Math.min(1, (e.clientX - rect.left) / rect.width));

        // Show tooltip with timestamp
        const timestamp = this.interpolateTimestamp(position);
        bar.title = this.formatTimestamp(timestamp);
    }

    seekToPosition(position) {
        if (!this.data) return;

        // Find the bucket at this position
        const bucketIndex = Math.floor(position * this.data.buckets.length);
        const bucket = this.data.buckets[Math.min(bucketIndex, this.data.buckets.length - 1)];

        if (bucket) {
            // Update the offset input and navigate
            const offsetInput = document.getElementById('offset');
            const currentOffsetEl = document.getElementById('tm-current-offset');

            if (offsetInput) {
                offsetInput.value = bucket.start_offset;
            }
            if (currentOffsetEl) {
                currentOffsetEl.textContent = bucket.start_offset;
            }

            // Submit the form to navigate
            const form = document.getElementById('browse-form');
            if (form) {
                form.submit();
            }
        }
    }

    togglePlayback() {
        if (this.isPlaying) {
            this.pause();
        } else {
            this.play();
        }
    }

    play() {
        if (!this.data) return;

        this.isPlaying = true;
        this.updatePlayButton();

        // Calculate step size based on speed
        const stepSize = 0.01 * this.playbackSpeed;
        const intervalMs = 100;

        this.playbackInterval = setInterval(() => {
            this.currentPosition += stepSize;

            if (this.currentPosition >= 1) {
                this.currentPosition = 1;
                this.pause();
            }

            this.updateCursor();
        }, intervalMs);
    }

    pause() {
        this.isPlaying = false;
        this.updatePlayButton();

        if (this.playbackInterval) {
            clearInterval(this.playbackInterval);
            this.playbackInterval = null;
        }
    }

    updatePlayButton() {
        const playIcon = document.getElementById('tm-play-icon');
        if (!playIcon) return;

        if (this.isPlaying) {
            // Show pause icon
            playIcon.innerHTML = '<path d="M5.5 3.5A1.5 1.5 0 0 1 7 5v6a1.5 1.5 0 0 1-3 0V5a1.5 1.5 0 0 1 1.5-1.5zm5 0A1.5 1.5 0 0 1 12 5v6a1.5 1.5 0 0 1-3 0V5a1.5 1.5 0 0 1 1.5-1.5z"/>';
        } else {
            // Show play icon
            playIcon.innerHTML = '<path d="M10.804 8 5 4.633v6.734L10.804 8zm.792-.696a.802.802 0 0 1 0 1.392l-6.363 3.692C4.713 12.69 4 12.345 4 11.692V4.308c0-.653.713-.998 1.233-.696l6.363 3.692z"/>';
        }
    }

    setSpeed(speed) {
        this.playbackSpeed = parseFloat(speed);

        // If playing, restart with new speed
        if (this.isPlaying) {
            this.pause();
            this.play();
        }
    }

    jumpToTime() {
        const jumpInput = document.getElementById('tm-jump-time');
        if (!jumpInput || !jumpInput.value || !this.data) return;

        const targetTime = new Date(jumpInput.value).getTime();
        const startTime = this.data.start_timestamp_ms;
        const endTime = this.data.end_timestamp_ms;

        // Calculate position
        const position = Math.max(0, Math.min(1,
            (targetTime - startTime) / (endTime - startTime)
        ));

        this.currentPosition = position;
        this.updateCursor();
        this.seekToPosition(position);
    }

    interpolateTimestamp(position) {
        if (!this.data) return Date.now();

        const startTime = this.data.start_timestamp_ms;
        const endTime = this.data.end_timestamp_ms;

        return startTime + (endTime - startTime) * position;
    }

    formatTimestamp(ms) {
        const date = new Date(ms);
        return date.toLocaleString('en-US', {
            month: 'short',
            day: 'numeric',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit'
        });
    }

    formatDuration(ms) {
        const hours = Math.floor(ms / (1000 * 60 * 60));
        const minutes = Math.floor((ms % (1000 * 60 * 60)) / (1000 * 60));

        if (hours > 24) {
            const days = Math.floor(hours / 24);
            return `${days}d ${hours % 24}h`;
        }
        if (hours > 0) {
            return `${hours}h ${minutes}m`;
        }
        return `${minutes}m`;
    }

    showError(message) {
        const container = document.getElementById('time-machine');
        if (container) {
            const errorEl = document.createElement('div');
            errorEl.className = 'time-machine-error';
            errorEl.textContent = message;
            container.querySelector('.time-machine-timeline')?.appendChild(errorEl);
        }
    }
}

// Initialize Time Machine
let timeMachine = null;
document.addEventListener('DOMContentLoaded', () => {
    if (document.getElementById('time-machine')) {
        timeMachine = new TimeMachine();
        window.timeMachine = timeMachine;
    }
});

// Consumer Lag History Chart
class LagHistoryChart {
    constructor() {
        this.data = null;
        this.currentRange = '1h';
        this.groupId = null;
        this.init();
    }

    init() {
        // Only initialize if chart container exists
        const container = document.getElementById('lag-history-chart');
        if (!container) return;

        // Extract group ID from URL
        const pathMatch = window.location.pathname.match(/\/consumer-groups\/([^\/]+)/);
        if (pathMatch) {
            this.groupId = decodeURIComponent(pathMatch[1]);
        }

        // Set up time range selector
        this.setupRangeSelector();

        // Load initial data
        this.loadData();
    }

    setupRangeSelector() {
        const buttons = document.querySelectorAll('.range-btn');
        buttons.forEach(btn => {
            btn.addEventListener('click', () => {
                // Update active state
                buttons.forEach(b => b.classList.remove('active'));
                btn.classList.add('active');

                // Load data for new range
                this.currentRange = btn.dataset.range;
                this.loadData();
            });
        });
    }

    async loadData() {
        if (!this.groupId) return;

        const chartContainer = document.getElementById('lag-history-chart');
        if (chartContainer) {
            chartContainer.innerHTML = '<div class="chart-loading">Loading lag history...</div>';
        }

        try {
            const url = `/api/consumer-groups/${encodeURIComponent(this.groupId)}/lag/history?range=${this.currentRange}`;
            const response = await fetch(url);

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }

            this.data = await response.json();
            this.render();
        } catch (error) {
            console.error('Failed to load lag history:', error);
            if (chartContainer) {
                chartContainer.innerHTML = '<div class="chart-error">Failed to load lag history data</div>';
            }
        }
    }

    render() {
        if (!this.data) return;

        this.renderVelocityIndicator();
        this.renderChart();
        this.renderSparklines();
    }

    renderVelocityIndicator() {
        const velocityEl = document.getElementById('lag-velocity');
        if (!velocityEl) return;

        const { overall_velocity, current_total_lag, threshold_exceeded } = this.data;

        let icon, text, colorClass;
        switch (overall_velocity) {
            case 'growing':
                icon = '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="12" y1="19" x2="12" y2="5"/><polyline points="5 12 12 5 19 12"/></svg>';
                text = 'Growing';
                colorClass = 'velocity-growing';
                break;
            case 'shrinking':
                icon = '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="12" y1="5" x2="12" y2="19"/><polyline points="19 12 12 19 5 12"/></svg>';
                text = 'Shrinking';
                colorClass = 'velocity-shrinking';
                break;
            default:
                icon = '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="5" y1="12" x2="19" y2="12"/></svg>';
                text = 'Stable';
                colorClass = 'velocity-stable';
        }

        velocityEl.className = `lag-velocity-indicator ${colorClass}${threshold_exceeded ? ' threshold-exceeded' : ''}`;
        velocityEl.innerHTML = `
            <span class="velocity-icon">${icon}</span>
            <span class="velocity-text">Lag is ${text.toLowerCase()}</span>
            <span class="velocity-value">${current_total_lag.toLocaleString()} total lag</span>
            ${threshold_exceeded ? '<span class="velocity-alert">Threshold exceeded!</span>' : ''}
        `;
    }

    renderChart() {
        const container = document.getElementById('lag-history-chart');
        if (!container || !this.data || !this.data.history.length) return;

        const { history, start_timestamp, end_timestamp } = this.data;

        // Find min/max for scaling
        const values = history.map(p => p.lag);
        const minLag = Math.min(...values);
        const maxLag = Math.max(...values);
        const range = maxLag - minLag || 1;

        // Build SVG chart
        const width = 800;
        const height = 200;
        const padding = { top: 20, right: 20, bottom: 30, left: 60 };
        const chartWidth = width - padding.left - padding.right;
        const chartHeight = height - padding.top - padding.bottom;

        // Create path data
        const points = history.map((point, i) => {
            const x = padding.left + (i / (history.length - 1)) * chartWidth;
            const y = padding.top + chartHeight - ((point.lag - minLag) / range) * chartHeight;
            return `${x},${y}`;
        });

        const linePath = `M ${points.join(' L ')}`;

        // Create area fill path
        const areaPath = `M ${padding.left},${padding.top + chartHeight} L ${points.join(' L ')} L ${padding.left + chartWidth},${padding.top + chartHeight} Z`;

        // Y-axis labels
        const yLabels = [0, 0.25, 0.5, 0.75, 1].map(pct => {
            const value = minLag + range * (1 - pct);
            const y = padding.top + chartHeight * pct;
            return `<text x="${padding.left - 10}" y="${y + 4}" text-anchor="end" class="chart-label">${this.formatLag(value)}</text>`;
        }).join('');

        // X-axis labels
        const startDate = new Date(this.data.start_timestamp_ms);
        const endDate = new Date(this.data.end_timestamp_ms);
        const xLabels = [0, 0.5, 1].map(pct => {
            const x = padding.left + chartWidth * pct;
            const time = new Date(startDate.getTime() + (endDate.getTime() - startDate.getTime()) * pct);
            return `<text x="${x}" y="${height - 5}" text-anchor="middle" class="chart-label">${this.formatTime(time)}</text>`;
        }).join('');

        // Grid lines
        const gridLines = [0.25, 0.5, 0.75].map(pct => {
            const y = padding.top + chartHeight * pct;
            return `<line x1="${padding.left}" y1="${y}" x2="${width - padding.right}" y2="${y}" class="chart-grid"/>`;
        }).join('');

        container.innerHTML = `
            <svg viewBox="0 0 ${width} ${height}" class="lag-history-svg">
                <defs>
                    <linearGradient id="lagGradient" x1="0%" y1="0%" x2="0%" y2="100%">
                        <stop offset="0%" style="stop-color: var(--accent-primary); stop-opacity: 0.3"/>
                        <stop offset="100%" style="stop-color: var(--accent-primary); stop-opacity: 0"/>
                    </linearGradient>
                </defs>
                ${gridLines}
                <path d="${areaPath}" fill="url(#lagGradient)"/>
                <path d="${linePath}" fill="none" stroke="var(--accent-primary)" stroke-width="2"/>
                ${yLabels}
                ${xLabels}
                ${this.data.alert_threshold ? this.renderThresholdLine(padding, chartWidth, chartHeight, minLag, range) : ''}
            </svg>
        `;
    }

    renderThresholdLine(padding, chartWidth, chartHeight, minLag, range) {
        const threshold = this.data.alert_threshold;
        if (threshold < minLag || threshold > minLag + range) return '';

        const y = padding.top + chartHeight - ((threshold - minLag) / range) * chartHeight;
        return `
            <line x1="${padding.left}" y1="${y}" x2="${800 - padding.right}" y2="${y}"
                  class="threshold-line" stroke-dasharray="5,5"/>
            <text x="${800 - padding.right + 5}" y="${y + 4}" class="threshold-label">Alert</text>
        `;
    }

    renderSparklines() {
        const container = document.getElementById('partition-sparklines');
        if (!container || !this.data || !this.data.partitions.length) return;

        const partitionCards = this.data.partitions.map(partition => {
            const sparklineSvg = this.createSparkline(partition.sparkline);
            const velocityClass = `velocity-${partition.velocity}`;
            const velocityIcon = this.getVelocityIcon(partition.velocity);

            return `
                <div class="sparkline-card">
                    <div class="sparkline-header">
                        <span class="sparkline-topic">${partition.topic}</span>
                        <span class="sparkline-partition">P${partition.partition}</span>
                        <span class="sparkline-velocity ${velocityClass}">${velocityIcon}</span>
                    </div>
                    <div class="sparkline-chart">${sparklineSvg}</div>
                    <div class="sparkline-stats">
                        <span class="sparkline-current">Current: <strong>${partition.current_lag.toLocaleString()}</strong></span>
                        <span class="sparkline-range">
                            <span class="stat-min">Min: ${partition.min_lag.toLocaleString()}</span>
                            <span class="stat-max">Max: ${partition.max_lag.toLocaleString()}</span>
                        </span>
                    </div>
                </div>
            `;
        }).join('');

        container.innerHTML = partitionCards;
    }

    createSparkline(values) {
        if (!values || values.length < 2) return '';

        const width = 150;
        const height = 30;
        const min = Math.min(...values);
        const max = Math.max(...values);
        const range = max - min || 1;

        const points = values.map((v, i) => {
            const x = (i / (values.length - 1)) * width;
            const y = height - ((v - min) / range) * height;
            return `${x},${y}`;
        });

        return `
            <svg viewBox="0 0 ${width} ${height}" class="sparkline-svg">
                <polyline points="${points.join(' ')}" fill="none" stroke="var(--accent-primary)" stroke-width="1.5"/>
            </svg>
        `;
    }

    getVelocityIcon(velocity) {
        switch (velocity) {
            case 'growing':
                return '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="12" y1="19" x2="12" y2="5"/><polyline points="5 12 12 5 19 12"/></svg>';
            case 'shrinking':
                return '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="12" y1="5" x2="12" y2="19"/><polyline points="19 12 12 19 5 12"/></svg>';
            default:
                return '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="5" y1="12" x2="19" y2="12"/></svg>';
        }
    }

    formatLag(value) {
        if (value >= 1000000) return `${(value / 1000000).toFixed(1)}M`;
        if (value >= 1000) return `${(value / 1000).toFixed(1)}K`;
        return Math.round(value).toString();
    }

    formatTime(date) {
        return date.toLocaleString('en-US', {
            month: 'short',
            day: 'numeric',
            hour: '2-digit',
            minute: '2-digit'
        });
    }

    exportCsv() {
        if (!this.groupId) return;

        const url = `/api/consumer-groups/${encodeURIComponent(this.groupId)}/lag/history/export?range=${this.currentRange}`;
        window.location.href = url;
    }
}

// Initialize Lag History Chart
let lagHistoryChart = null;
document.addEventListener('DOMContentLoaded', () => {
    if (document.getElementById('lag-history-chart')) {
        lagHistoryChart = new LagHistoryChart();
        window.lagHistoryChart = lagHistoryChart;
    }
});

// Focus search input helper
function focusSearch() {
    // Try various search inputs
    const searchInputs = [
        document.getElementById('search-input'),
        document.querySelector('.search-input'),
        document.querySelector('input[type="search"]'),
        document.querySelector('input[placeholder*="search" i]'),
        document.querySelector('input[placeholder*="filter" i]')
    ];

    for (const input of searchInputs) {
        if (input && input.offsetParent !== null) {
            input.focus();
            input.select();
            return true;
        }
    }
    return false;
}

// Keyboard shortcuts
document.addEventListener('keydown', (e) => {
    // Don't intercept if typing in an input
    const isInputFocused = document.activeElement &&
        (document.activeElement.tagName === 'INPUT' ||
         document.activeElement.tagName === 'TEXTAREA' ||
         document.activeElement.isContentEditable);

    // Ctrl/Cmd + K for command palette (always active)
    if ((e.ctrlKey || e.metaKey) && e.key === 'k') {
        e.preventDefault();
        if (window.commandPalette) {
            window.commandPalette.toggle();
        }
        return;
    }

    // Escape to close modals
    if (e.key === 'Escape') {
        if (window.commandPalette && window.commandPalette.isOpen) {
            window.commandPalette.close();
            return;
        }
        document.querySelectorAll('.modal-overlay:not(.hidden)').forEach(modal => {
            modal.classList.add('hidden');
        });
        // Blur any focused input
        if (isInputFocused) {
            document.activeElement.blur();
        }
        return;
    }

    // Skip vim keys if input is focused
    if (isInputFocused) return;

    // Vim navigation keys
    if (vimNavigator && vimNavigator.isEnabled()) {
        switch (e.key) {
            case 'j':
                if (vimNavigator.moveDown()) e.preventDefault();
                break;
            case 'k':
                if (vimNavigator.moveUp()) e.preventDefault();
                break;
            case 'g':
                if (!e.shiftKey && vimNavigator.goToTop()) e.preventDefault();
                break;
            case 'G':
                if (vimNavigator.goToBottom()) e.preventDefault();
                break;
            case 'Enter':
                if (vimNavigator.selectCurrent()) e.preventDefault();
                break;
        }
    }

    // / for search focus (vim-like)
    if (e.key === '/' && !e.ctrlKey && !e.metaKey) {
        if (focusSearch()) {
            e.preventDefault();
        }
    }

    // Toggle vim mode with Ctrl+Shift+V
    if (e.ctrlKey && e.shiftKey && e.key === 'V') {
        e.preventDefault();
        if (vimNavigator) {
            vimNavigator.toggle();
        }
    }
});

// Auto-refresh toggle
let autoRefreshEnabled = true;
let autoRefreshInterval = null;

function toggleAutoRefresh() {
    autoRefreshEnabled = !autoRefreshEnabled;
    const btn = document.getElementById('auto-refresh-btn');
    if (btn) {
        btn.textContent = autoRefreshEnabled ? 'Pause' : 'Resume';
        btn.classList.toggle('active', autoRefreshEnabled);
    }
}

// Activity feed
const activityFeed = document.getElementById('activity-feed');
const maxActivityItems = 20;

function addActivityItem(event) {
    if (!activityFeed) return;

    // Remove placeholder if present
    const placeholder = activityFeed.querySelector('.muted');
    if (placeholder) placeholder.remove();

    const item = document.createElement('div');
    item.className = 'activity-item';
    item.innerHTML = `
        <span class="activity-time">${new Date().toLocaleTimeString()}</span>
        <span class="activity-text">${event}</span>
    `;

    activityFeed.insertBefore(item, activityFeed.firstChild);

    // Limit items
    while (activityFeed.children.length > maxActivityItems) {
        activityFeed.removeChild(activityFeed.lastChild);
    }
}

// Confirmation dialogs
function confirm(message, callback) {
    if (window.confirm(message)) {
        callback();
    }
}

// Form validation
function validateTopicName(name) {
    const pattern = /^[a-zA-Z0-9._-]+$/;
    if (!pattern.test(name)) {
        return 'Topic name can only contain letters, numbers, dots, underscores, and hyphens';
    }
    if (name.length > 249) {
        return 'Topic name must be 249 characters or less';
    }
    return null;
}

// Dynamic table updates
function updateTableRow(tableId, rowId, data, columns) {
    const table = document.getElementById(tableId);
    if (!table) return;

    let row = table.querySelector(`tr[data-id="${rowId}"]`);

    if (!row) {
        row = document.createElement('tr');
        row.dataset.id = rowId;
        table.querySelector('tbody').appendChild(row);
    }

    row.innerHTML = columns.map(col => {
        const value = data[col.key];
        if (col.render) {
            return `<td>${col.render(value, data)}</td>`;
        }
        return `<td>${value ?? '-'}</td>`;
    }).join('');
}

// Dark/light mode toggle (for future use)
function toggleTheme() {
    document.body.classList.toggle('light-mode');
    localStorage.setItem('theme', document.body.classList.contains('light-mode') ? 'light' : 'dark');
}

// Initialize theme from storage
if (localStorage.getItem('theme') === 'light') {
    document.body.classList.add('light-mode');
}

// Export utilities for use in other scripts
window.StreamlineUI = {
    showToast,
    formatBytes,
    formatNumber,
    timeAgo,
    sortTable,
    filterTable,
    toggleAutoRefresh,
    addActivityItem,
    updateTableRow,
    validateTopicName,
    toggleTheme
};

// Debug logging
if (window.location.search.includes('debug')) {
    console.log('Streamline UI Debug Mode enabled');
    window.DEBUG = true;
}
