/**
 * Shared Language Selector Component
 * Centralized logic for language switching across extensions.
 */
class LanguageSelector extends HTMLElement {
    constructor() {
        super();
        this.languages = {
            'en': 'English',
            'es': 'Español',
            'fr': 'Français',
            'de': 'Deutsch',
            'it': 'Italiano',
            'pt': 'Português',
            'zh': '中文',
            'ja': '日本語',
            'ar': 'العربية',
            'ru': 'Русский',
            'ko': '한국어',
            'tr': 'Türkçe',
            'nl': 'Nederlands',
        };
    }

    connectedCallback() {
        this.render();
    }

    getCurrentLang() {
        const urlParams = new URLSearchParams(window.location.search);
        return urlParams.get('language') || document.documentElement.lang || 'en';
    }

    render() {
        const currentLang = this.getCurrentLang();
        const currentLangName = this.languages[currentLang] || currentLang.toUpperCase();

        this.innerHTML = `
            <div class="ds-lang-selector">
                <button class="ds-lang-btn" id="ds-lang-trigger" aria-haspopup="true" aria-expanded="false">
                    <span class="ds-lang-label">${currentLang.toUpperCase()}</span>
                    <svg class="ds-icon-chevron" viewBox="0 0 20 20" fill="currentColor">
                        <path fill-rule="evenodd" d="M5.293 7.293a1 1 0 011.414 0L10 10.586l3.293-3.293a1 1 0 111.414 1.414l-4 4a1 1 0 01-1.414 0l-4-4a1 1 0 010-1.414z" clip-rule="evenodd" />
                    </svg>
                </button>
                <div class="ds-lang-dropdown" id="ds-lang-menu" role="menu">
                    ${Object.entries(this.languages).map(([code, name]) => `
                        <a href="?language=${code}" 
                           class="ds-lang-option ${code === currentLang ? 'active' : ''}" 
                           role="menuitem"
                           data-lang="${code}">
                            ${name}
                        </a>
                    `).join('')}
                </div>
            </div>
        `;

        this.setupEventListeners();
    }

    setupEventListeners() {
        const trigger = this.querySelector('#ds-lang-trigger');
        const menu = this.querySelector('#ds-lang-menu');

        trigger.addEventListener('click', (e) => {
            e.stopPropagation();
            const expanded = trigger.getAttribute('aria-expanded') === 'true';
            trigger.setAttribute('aria-expanded', !expanded);
            menu.classList.toggle('show');
        });

        document.addEventListener('click', (e) => {
            if (!this.contains(e.target)) {
                trigger.setAttribute('aria-expanded', 'false');
                menu.classList.remove('show');
            }
        });
    }
}

if (!customElements.get('ds-language-selector')) {
    customElements.define('ds-language-selector', LanguageSelector);
}
