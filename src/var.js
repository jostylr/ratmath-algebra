/**
 * var.js
 *
 * Variable management and mini-language features for the calculator.
 * Supports single-character variables, function definitions, and special functions like SUM, PROD, SEQ.
 */

import { Rational, RationalInterval, Integer, BaseSystem } from "@ratmath/core";
import { Parser } from "@ratmath/parser";
import { PackageRegistry, getPackageInfo, resolveDependencies, getPackagesHelpText } from "./package-registry.js";
import { getHelpText, hasHelpTopic, getHelpTopicsText, getHelpIntroText } from "./help-registry.js";

export class VariableManager {
    constructor() {
        this.variables = new Map(); // Store single-character variables
        this.functions = new Map(); // Store function definitions
        this.modules = new Map();   // Store loaded modules { name: string, content: object }
        this.loadedPackages = new Set(); // Track loaded packages from registry
        this.inputBase = null; // Base system for interpreting numbers without explicit base notation
        this.customBases = new Map(); // Store custom base definitions
        this.decorations = new Map(); // Store property decorations for variables/functions: Map<name, Map<propName, value>>

        // Regex patterns for validation
        // Updaed to support namespacing: @@Module@Name
        // Variable: starts with lowercase or underscore, optional @@Mod@ prefix
        this.variablePattern = /^(?:@@[a-zA-Z0-9_]+@)?(?:@?([_a-z][a-zA-Z0-9_]*))$/;
        // Function: starts with uppercase, optional @@Mod@ prefix
        this.functionPattern = /^(?:@@[a-zA-Z0-9_]+@)?(?:@?([A-Z][a-zA-Z0-9_]*))$/;
    }

    /**
     * Normalize a name for case-insensitive lookup after first character.
     * - lowercase start (variable) → all lowercase (aVar → avar)
     * - Uppercase start (function) → ALL UPPERCASE (Avar → AVAR)
     * - underscore prefix → second char determines case (_Fun → _FUN, _var → _var)
     * - @@Module@ prefix preserved, rest normalized
     * @param {string} name - Name to normalize
     * @returns {string} - Normalized name
     */
    normalizeName(name) {
        if (!name) return name;

        // Internal @@Anon@ names should not be normalized - they're system-generated
        if (name.startsWith("@@Anon@")) {
            return name;
        }

        // Handle @@ module prefix: @@Module@Name
        const moduleMatch = name.match(/^(@@[a-zA-Z0-9_]+@)(.+)$/);
        if (moduleMatch) {
            const [, prefix, rest] = moduleMatch;
            return prefix.toUpperCase() + this.normalizeName(rest);
        }

        // Handle @ prefix (strip it for normalization, add back)
        if (name.startsWith("@") && !name.startsWith("@@")) {
            return "@" + this.normalizeName(name.substring(1));
        }

        // Underscore prefix: second character determines case
        // _Fun → _FUN (uppercase), _var → _var (lowercase)
        if (name.startsWith("_") && name.length > 1) {
            const secondChar = name.charAt(1);
            if (secondChar >= 'A' && secondChar <= 'Z') {
                return "_" + name.substring(1).toUpperCase();
            } else {
                return "_" + name.substring(1).toLowerCase();
            }
        }

        // First char determines case: lowercase → all lower, Uppercase → ALL UPPER
        const firstChar = name.charAt(0);
        if (firstChar >= 'a' && firstChar <= 'z') {
            return name.toLowerCase();
        } else if (firstChar >= 'A' && firstChar <= 'Z') {
            return name.toUpperCase();
        }

        return name;
    }

    /**
     * Set a variable with strict case validation
     * @param {string} name - Variable name (must start with lowercase or @lowercase)
     * @param {any} value - Value to set
     */
    setVariable(name, value) {
        if (!this.variablePattern.test(name)) {
            // Check if it looks like a function name
            if (this.functionPattern.test(name)) {
                throw new Error(`Invalid variable name '${name}'. Function names (starting with Uppercase) cannot be assigned values directly. Use '${name}(...) -> ...' to define a function.`);
            }
            throw new Error(`Invalid variable name '${name}'. Variables must start with a lowercase letter, underscore, or @lowercase/@underscore.`);
        }
        // Normalize: strip leading @ and apply case normalization
        let normalizedName = name.startsWith("@") ? name.substring(1) : name;
        normalizedName = this.normalizeName(normalizedName);
        this.variables.set(normalizedName, value);
    }

    /**
     * Get a variable value with case normalization
     * @param {string} name - Variable name
     * @returns {any} - Variable value or undefined
     */
    getVariable(name) {
        let normalizedName = name.startsWith("@") ? name.substring(1) : name;
        normalizedName = this.normalizeName(normalizedName);
        return this.variables.get(normalizedName);
    }

    /**
     * Check if a variable exists with case normalization
     * @param {string} name - Variable name
     * @returns {boolean}
     */
    hasVariable(name) {
        let normalizedName = name.startsWith("@") ? name.substring(1) : name;
        normalizedName = this.normalizeName(normalizedName);
        return this.variables.has(normalizedName);
    }

    /**
     * Try to evaluate oracle arithmetic expressions like c+c, 2*c, c*2, etc.
     * Returns the result oracle if successful, null if not an oracle expression.
     * @param {string} expr - Expression to evaluate
     * @param {Array} scopeChain - Scope chain for variable lookup
     * @returns {any|null} - Result oracle or null
     */
    tryOracleArithmetic(expr, scopeChain = []) {
        // Helper to get variable value (checking scopes then globals)
        const getVar = (name) => {
            const normalized = this.normalizeName(name.startsWith('@') ? name.substring(1) : name);
            for (const scope of scopeChain) {
                if (scope.has(normalized)) return scope.get(normalized);
            }
            if (this.variables.has(normalized)) return this.variables.get(normalized);
            return undefined;
        };

        // Check if value is an oracle
        const isOracle = (val) => typeof val === 'function' && val.yes;

        // Try to parse value - could be variable name or number
        const parseValue = (token) => {
            token = token.trim();
            if (!token) return null;

            // Check if it's a variable
            if (/^@?[a-zA-Z_][a-zA-Z0-9_]*$/.test(token)) {
                const val = getVar(token);
                if (val !== undefined) return val;
            }

            // Try to parse as number
            const num = parseFloat(token);
            if (!isNaN(num)) return num;

            return null;
        };

        // Match simple binary expressions: a OP b
        // Operators: +, -, *, /
        const binaryMatch = expr.match(/^([^+\-*/]+)([+\-*/])([^+\-*/]+)$/);
        if (binaryMatch) {
            const [, leftStr, op, rightStr] = binaryMatch;
            const left = parseValue(leftStr);
            const right = parseValue(rightStr);

            if (left === null || right === null) return null;

            // At least one must be an oracle
            if (!isOracle(left) && !isOracle(right)) return null;

            // Both are oracles or one is oracle and one is number
            // Use the oracle's arithmetic methods
            let oracle = isOracle(left) ? left : right;
            let other = isOracle(left) ? right : left;
            let leftIsOracle = isOracle(left);

            // If the non-oracle is a number, we need to convert it
            // The oracle's methods handle this conversion
            switch (op) {
                case '+':
                    return oracle.add(other);
                case '-':
                    if (leftIsOracle) {
                        return oracle.subtract(other);
                    } else {
                        // other - oracle = -(oracle - other)
                        return oracle.subtract(other).negate();
                    }
                case '*':
                    return oracle.multiply(other);
                case '/':
                    if (leftIsOracle) {
                        return oracle.divide(other);
                    } else {
                        // other / oracle - need reciprocal, not directly supported
                        // Fall through to normal parsing
                        return null;
                    }
            }
        }

        // Match unary negation: -a
        const unaryMatch = expr.match(/^-([a-zA-Z_@][a-zA-Z0-9_]*)$/);
        if (unaryMatch) {
            const val = parseValue(unaryMatch[1]);
            if (val && isOracle(val)) {
                return val.negate();
            }
        }

        return null;
    }

    /**
     * Define a function with strict case validation
     * @param {string} name - Function name (must start with Uppercase or @Uppercase)
     * @param {string[]} params - List of parameter names
     * @param {string} body - Function body expression
     * @param {string} doc - Documentation string
     * @param {object} defaults - Map of parameter name to default value expression
     */
    defineFunction(name, params, body, doc = "", defaults = {}) {
        if (!this.functionPattern.test(name)) {
            // Check if it looks like a variable name
            if (this.variablePattern.test(name)) {
                throw new Error(`Invalid function name '${name}'. Function definitions must use names starting with an Uppercase letter or @Uppercase.`);
            }
            throw new Error(`Invalid function name '${name}'. Functions must start with an Uppercase letter or @Uppercase.`);
        }
        const normalizedName = this.normalizeName(name);
        this.functions.set(normalizedName, { params, body, doc, type: 'def', defaults });
    }

    /**
     * Get a function definition with case normalization
     * @param {string} name - Function name
     * @returns {object|undefined} - Function definition or undefined
     */
    getFunction(name) {
        const normalizedName = this.normalizeName(name);
        return this.functions.get(normalizedName);
    }

    /**
     * Check if a function exists with case normalization
     * @param {string} name - Function name
     * @returns {boolean}
     */
    hasFunction(name) {
        const normalizedName = this.normalizeName(name);
        return this.functions.has(normalizedName);
    }

    /**
     * Set a function definition with case normalization
     * @param {string} name - Function name
     * @param {object} def - Function definition
     */
    setFunction(name, def) {
        const normalizedName = this.normalizeName(name);
        this.functions.set(normalizedName, def);
    }

    /**
     * Register a JavaScript function
     * @param {string} name - Function name
     * @param {Function} handler - JS function to execute
     * @param {string[]} params - Parameter names (for help/signature)
     * @param {string} doc - Documentation string
     */
    registerJSFunction(name, handler, params, doc = "") {
        if (!this.functionPattern.test(name)) {
            throw new Error(`Invalid function name '${name}'. Functions must start with an Uppercase letter.`);
        }
        const normalizedName = this.normalizeName(name);
        this.functions.set(normalizedName, { type: 'js', handler, params, doc });
    }

    /**
     * Get help/documentation for a function, package, or topic
     * @param {string} [name] - Function, package, or topic name (optional)
     * @returns {string} - Help text
     */
    getHelp(name) {
        if (name) {
            const lower = name.toLowerCase();

            // Check for "packages" keyword
            if (lower === "packages") {
                return getPackagesHelpText();
            }

            // Check for "topics" keyword - list all help topics
            if (lower === "topics") {
                return getHelpTopicsText();
            }

            // Check help registry for topics (core, logic, list, syntax, trig, etc.)
            const helpText = getHelpText(lower);
            if (helpText) {
                return helpText;
            }

            // Check if it's a package name
            const pkgInfo = getPackageInfo(name);
            if (pkgInfo) {
                const loaded = this.loadedPackages.has(pkgInfo.key) ? " [LOADED]" : "";
                return `${pkgInfo.name}${loaded}\n${pkgInfo.help || pkgInfo.description}`;
            }

            // Check for function
            const normalized = this.normalizeName(name.startsWith("@@") ? name : (name.startsWith("@") ? name.substring(1) : name));
            if (this.functions.has(normalized)) {
                const f = this.functions.get(normalized);
                const sig = `${normalized}(${f.params.join(", ")})`;
                return `${sig}\n${f.doc || "No documentation available."}`;
            }
            return `'${name}' not found. Type HELP topics or HELP packages.`;
        }

        // No argument - show essential intro
        return getHelpIntroText();
    }

    /**
     * Check if a package is loaded
     * @param {string} packageName - Package name (case-insensitive)
     * @returns {boolean}
     */
    isPackageLoaded(packageName) {
        return this.loadedPackages.has(packageName.toLowerCase());
    }

    /**
     * Get set of loaded packages
     * @returns {Set<string>}
     */
    getLoadedPackages() {
        return new Set(this.loadedPackages);
    }

    /**
     * Mark a package as loaded (called after loadModule for registry packages)
     * @param {string} packageName - Package name
     */
    markPackageLoaded(packageName) {
        this.loadedPackages.add(packageName.toLowerCase());
    }

    /**
     * Mark a package as unloaded
     * @param {string} packageName - Package name
     */
    markPackageUnloaded(packageName) {
        this.loadedPackages.delete(packageName.toLowerCase());
    }

    /**
     * Load a module into the current namespace
     * @param {string} moduleName - Name of the module (e.g. "Core")
     * @param {object} scope - Object containing vars and functions to load
     */
    loadModule(moduleName, scope) {
        const normalizedModuleName = moduleName.toUpperCase();
        const prefix = `@@${normalizedModuleName}@`;

        // Register functions with normalized names
        if (scope.functions) {
            for (const [name, def] of Object.entries(scope.functions)) {
                const normalizedName = this.normalizeName(name);
                const qualifiedName = `${prefix}${normalizedName}`;
                // Normalize: some modules use 'body' instead of 'handler' for JS functions
                const normalizedDef = { ...def };
                if (normalizedDef.body && !normalizedDef.handler) {
                    normalizedDef.handler = normalizedDef.body;
                }
                this.functions.set(qualifiedName, normalizedDef);
                this.functions.set(normalizedName, { ...normalizedDef, isImported: true, module: normalizedModuleName });
            }
        }

        // Register variables with normalized names
        if (scope.variables) {
            for (const [name, val] of Object.entries(scope.variables)) {
                const normalizedName = this.normalizeName(name);
                const qualifiedName = `${prefix}${normalizedName}`;
                this.variables.set(qualifiedName, val);
                this.variables.set(normalizedName, val);
            }
        }

        this.modules.set(moduleName, scope);
        return `Module '${moduleName}' loaded.`;
    }

    /**
     * Unload a module (remove imported aliases)
     */
    unloadModule(moduleName) {
        if (!this.modules.has(moduleName)) return `Module '${moduleName}' not loaded.`;

        let count = 0;
        // Remove functions tagged with this module (aliases)
        for (const [name, def] of this.functions) {
            if (def.isImported && def.module === moduleName) {
                this.functions.delete(name);
                count++;
            }
        }
        // Remove variables? (Metadata for vars needed)
        // For now, only removing functions is safer as vars are simple values.
        // We might want to track refined vars.

        this.modules.delete(moduleName);
        return `Module '${moduleName}' unloaded (${count} functions removed).`;
    }

    /**
     * Set the input base system for number interpretation
     * @param {BaseSystem} baseSystem - The base system to use for input
     */
    setInputBase(baseSystem) {
        this.inputBase = baseSystem;
    }

    static BASE_RESERVED_CHARS = new Set([".", "/", "#", "~", "_", "^", "+", "-"]);
    static SAFE_UNQUOTED_BASE_CHARS = /[0-9A-Za-z@&./#~_^+-]/;
    static DEFAULT_BASE_DIGITS = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ@&";
    static BASE_MODE_ALIASES = new Map([
        ["mixed", 1], ["..", 1],
        ["repeat", 2], [".", 2], ["#", 2], ["radix", 2],
        ["cf", 3], [".~", 3],
        ["cf_explicit", 4], ["~", 4],
        ["shifted", 5], ["_^", 5], ["^", 5],
        ["fraction", 6], ["/", 6], ["improper", 6],
    ]);
    static BASE_EXPANSION_LIMIT = 20;

    _toRationalValue(value) {
        if (value instanceof Integer) return value.toRational();
        if (value instanceof Rational) return value;
        if (value instanceof RationalInterval) {
            if (value.low.equals(value.high)) return value.low;
            throw new Error("Expected a single numeric value, not an interval");
        }
        throw new Error("Expected a numeric value");
    }

    _stringLiteralValue(src) {
        if (typeof src !== "string" || src.length < 2 || src[0] !== '"' || src[src.length - 1] !== '"') {
            throw new Error("Expected a quoted string");
        }
        return src
            .substring(1, src.length - 1)
            .replace(/\\u([0-9a-fA-F]{4})/g, (_, hex) => String.fromCharCode(parseInt(hex, 16)))
            .replace(/\\"/g, '"')
            .replace(/\\\\/g, "\\");
    }

    _groupDigits(intStr) {
        if (!intStr) return intStr;
        const sign = intStr.startsWith("-") ? "-" : "";
        const body = sign ? intStr.substring(1) : intStr;
        if (body.length <= 3) return intStr;
        let out = "";
        for (let i = 0; i < body.length; i++) {
            if (i > 0 && (body.length - i) % 3 === 0) out += "_";
            out += body[i];
        }
        return sign + out;
    }

    _groupDigitRuns(text, baseSystem) {
        if (!text) return text;
        let out = "";
        let run = "";
        const flush = () => {
            if (!run.length) return;
            if (run.length <= 3) {
                out += run;
            } else {
                for (let i = 0; i < run.length; i++) {
                    if (i > 0 && i % 3 === 0) out += "_";
                    out += run[i];
                }
            }
            run = "";
        };
        for (const ch of text) {
            if (baseSystem.charMap.has(ch)) {
                run += ch;
            } else {
                flush();
                out += ch;
            }
        }
        flush();
        return out;
    }

    _shortenRepeatingExpansion(expansion, limit = VariableManager.BASE_EXPANSION_LIMIT) {
        if (typeof expansion !== "string") return expansion;

        if (!expansion.includes("#")) {
            if (expansion.length > limit + 2) {
                const dotIndex = expansion.indexOf(".");
                if (dotIndex !== -1 && expansion.length - dotIndex - 1 > limit) {
                    return expansion.substring(0, dotIndex + limit + 1) + "...";
                }
            }
            return expansion;
        }

        if (expansion.endsWith("#0")) {
            const withoutRepeating = expansion.substring(0, expansion.length - 2);
            if (withoutRepeating.length > limit + 2) {
                const dotIndex = withoutRepeating.indexOf(".");
                if (dotIndex !== -1 && withoutRepeating.length - dotIndex - 1 > limit) {
                    return withoutRepeating.substring(0, dotIndex + limit + 1) + "...";
                }
            }
            return withoutRepeating;
        }

        if (expansion.length > limit + 2) {
            const hashIndex = expansion.indexOf("#");
            const beforeHash = expansion.substring(0, hashIndex);
            const afterHash = expansion.substring(hashIndex + 1);

            if (beforeHash.length > limit + 1) {
                return beforeHash.substring(0, limit + 1) + "...";
            }

            const remainingSpace = limit + 2 - beforeHash.length;
            if (remainingSpace <= 1) {
                return beforeHash + "#...";
            }
            if (afterHash.length > remainingSpace - 1) {
                return beforeHash + "#" + afterHash.substring(0, remainingSpace - 1) + "...";
            }
        }

        return expansion;
    }

    _groupRadixExpansion(expansion, baseSystem) {
        if (!expansion) return expansion;
        const sign = expansion.startsWith("-") ? "-" : "";
        const body = sign ? expansion.substring(1) : expansion;

        const hashIndex = body.indexOf("#");
        const beforeHash = hashIndex === -1 ? body : body.substring(0, hashIndex);
        const afterHash = hashIndex === -1 ? null : body.substring(hashIndex + 1);

        const dotIndex = beforeHash.indexOf(".");
        const integerPart = dotIndex === -1 ? beforeHash : beforeHash.substring(0, dotIndex);
        const fracPart = dotIndex === -1 ? null : beforeHash.substring(dotIndex + 1);

        const groupedInteger = this._groupDigits(integerPart);
        const groupedFrac = fracPart === null ? null : this._groupDigitRuns(fracPart, baseSystem);
        const groupedRepeat = afterHash === null ? null : this._groupDigitRuns(afterHash, baseSystem);

        let out = groupedInteger;
        if (groupedFrac !== null) out += "." + groupedFrac;
        if (groupedRepeat !== null) out += "#" + groupedRepeat;
        return sign + out;
    }

    _parseBaseInteger(str, baseSystem, { allowSign = true } = {}) {
        if (typeof str !== "string" || str.length === 0) {
            throw new Error("Expected base integer digits");
        }
        let s = str;
        let sign = 1n;
        if (allowSign && (s.startsWith("-") || s.startsWith("+"))) {
            sign = s[0] === "-" ? -1n : 1n;
            s = s.substring(1);
        }
        if (s.length === 0) throw new Error("Missing digits");
        if (s.startsWith("_") || s.endsWith("_")) throw new Error("Underscore cannot be leading or trailing");
        if (s.includes("__")) throw new Error("Consecutive underscores are not allowed");
        for (let i = 0; i < s.length; i++) {
            const ch = s[i];
            if (ch === "_") {
                const prev = s[i - 1];
                const next = s[i + 1];
                if (!baseSystem.charMap.has(prev) || !baseSystem.charMap.has(next)) {
                    throw new Error("Underscore separators must be between base digits");
                }
                continue;
            }
            if (!baseSystem.charMap.has(ch)) {
                throw new Error(`Invalid digit '${ch}' for ${baseSystem.name}`);
            }
        }
        const cleaned = s.replace(/_/g, "");
        const value = baseSystem.toDecimal(cleaned);
        return sign * value;
    }

    _parseSimpleBaseNumeral(str, baseSystem) {
        let s = str;
        let sign = 1n;
        if (s.startsWith("-") || s.startsWith("+")) {
            sign = s[0] === "-" ? -1n : 1n;
            s = s.substring(1);
        }
        if (s.length === 0) throw new Error("Missing digits");
        const dotParts = s.split(".");
        if (dotParts.length > 2) throw new Error("Too many radix points");
        const intPart = dotParts[0] === "" ? "0" : dotParts[0];
        const fracPart = dotParts.length === 2 ? dotParts[1] : "";
        const intVal = this._parseBaseInteger(intPart, baseSystem, { allowSign: false });
        let result = new Rational(intVal, 1n);
        if (fracPart.length > 0) {
            const fracVal = this._parseBaseInteger(fracPart, baseSystem, { allowSign: false });
            const denom = BigInt(baseSystem.base) ** BigInt(fracPart.replace(/_/g, "").length);
            const fracRat = new Rational(fracVal, denom);
            result = result.add(fracRat);
        }
        if (sign < 0n) result = result.negate();
        return result;
    }

    _continuedFractionFromTerms(terms) {
        if (!terms.length) throw new Error("Continued fraction requires at least one term");
        let acc = new Rational(terms[terms.length - 1], 1n);
        for (let i = terms.length - 2; i >= 0; i--) {
            if (acc.numerator === 0n) throw new Error("Invalid continued fraction term");
            acc = new Rational(terms[i], 1n).add(new Rational(1, 1).divide(acc));
        }
        return acc;
    }

    fromBaseString(baseStr, baseSystem) {
        if (typeof baseStr !== "string") throw new Error("FROMBASE expects a string input");
        let s = baseStr.trim();
        if (!s.length) throw new Error("Empty base numeral");

        if (s.startsWith("_")) throw new Error("Underscore cannot start a number");

        let expShift = null;
        const shiftIndex = s.indexOf("_^");
        if (shiftIndex !== -1) {
            if (s.indexOf("_^", shiftIndex + 2) !== -1) throw new Error("Only one _^ is allowed");
            if (s.includes("#") && shiftIndex < s.indexOf("#")) throw new Error("Radix shift must come after repeating section");
            expShift = s.substring(shiftIndex + 2);
            s = s.substring(0, shiftIndex);
            if (!expShift.length) throw new Error("Missing exponent after _^");
        }

        if (s.includes("~") && !s.includes(".~") && !s.startsWith("~")) {
            throw new Error("~ is only valid in continued fractions after .~");
        }

        let value;
        if (s.includes(".~")) {
            const explicit = s.startsWith("~");
            if (explicit) s = s.substring(1);
            const idx = s.indexOf(".~");
            const a0 = s.substring(0, idx);
            const tail = s.substring(idx + 2);
            if (!tail.length) throw new Error("Continued fraction requires terms after .~");
            const termStrs = [a0, ...tail.split("~")];
            if (termStrs.some((t) => t.length === 0)) throw new Error("Invalid continued fraction format");
            const terms = termStrs.map((t, i) => this._parseBaseInteger(t, baseSystem, { allowSign: i === 0 }));
            value = this._continuedFractionFromTerms(terms);
            if (explicit && terms[0] >= 0n) {
                // explicit form is representational only; numeric value is unchanged
            }
        } else if (s.includes("..")) {
            const parts = s.split("..");
            if (parts.length !== 2) throw new Error("Mixed number must have exactly one '..'");
            const whole = this._parseBaseInteger(parts[0], baseSystem, { allowSign: true });
            const fracParts = parts[1].split("/");
            if (fracParts.length !== 2) throw new Error("Mixed number requires Y/Z fractional part");
            if (fracParts[0].includes(".") || fracParts[1].includes(".")) {
                throw new Error("Mixed number fraction parts cannot contain radix points");
            }
            const num = this._parseBaseInteger(fracParts[0], baseSystem, { allowSign: false });
            const den = this._parseBaseInteger(fracParts[1], baseSystem, { allowSign: false });
            if (den === 0n) throw new Error("Denominator cannot be zero");
            let frac = new Rational(num, den);
            if (whole < 0n) frac = frac.negate();
            value = new Rational(whole, 1n).add(frac);
        } else if (s.includes("/")) {
            const parts = s.split("/");
            if (parts.length !== 2) throw new Error("Fraction must have exactly one '/'");
            if (parts[0].includes(".") || parts[1].includes(".")) {
                throw new Error("Fraction parts cannot contain radix points");
            }
            const num = this._parseBaseInteger(parts[0], baseSystem, { allowSign: true });
            const den = this._parseBaseInteger(parts[1], baseSystem, { allowSign: false });
            if (den === 0n) throw new Error("Denominator cannot be zero");
            value = new Rational(num, den);
            value._explicitFraction = true;
        } else if (s.includes("#")) {
            const parts = s.split("#");
            if (parts.length !== 2) throw new Error("Repeating form must have exactly one '#'");
            const prefix = parts[0];
            const repeat = parts[1];
            if (!repeat.length) throw new Error("Repeating block after # cannot be empty");
            if (repeat.includes(".") || repeat.includes("/") || repeat.includes("~") || repeat.includes("#")) {
                throw new Error("Repeating block must be plain digits");
            }
            const sign = prefix.startsWith("-") ? -1n : 1n;
            const unsignedPrefix = (prefix.startsWith("-") || prefix.startsWith("+")) ? prefix.substring(1) : prefix;
            const dot = unsignedPrefix.indexOf(".");
            const intStr = dot === -1 ? unsignedPrefix : unsignedPrefix.substring(0, dot);
            const nonRep = dot === -1 ? "" : unsignedPrefix.substring(dot + 1);
            const intVal = this._parseBaseInteger(intStr || "0", baseSystem, { allowSign: false });
            const nonRepVal = nonRep.length ? this._parseBaseInteger(nonRep, baseSystem, { allowSign: false }) : 0n;
            const repVal = this._parseBaseInteger(repeat, baseSystem, { allowSign: false });
            const B = BigInt(baseSystem.base);
            const m = BigInt(nonRep.replace(/_/g, "").length);
            const r = BigInt(repeat.replace(/_/g, "").length);
            let result = new Rational(intVal, 1n);
            if (m > 0n) result = result.add(new Rational(nonRepVal, B ** m));
            const repDen = (B ** m) * (B ** r - 1n);
            result = result.add(new Rational(repVal, repDen));
            value = sign < 0n ? result.negate() : result;
        } else {
            value = this._parseSimpleBaseNumeral(s, baseSystem);
        }

        if (expShift !== null) {
            const shift = this._parseBaseInteger(expShift, baseSystem, { allowSign: true });
            const B = new Rational(BigInt(baseSystem.base), 1n);
            const factor = shift >= 0n ? B.pow(shift) : new Rational(1, 1).divide(B.pow(-shift));
            value = value.multiply(factor);
        }

        if (value.denominator === 1n) return new Integer(value.numerator);
        return value;
    }

    _resolveBaseSpecValue(baseSpecValue) {
        const ensureReservedChars = (baseSystem) => {
            for (const ch of baseSystem.characters) {
                if (VariableManager.BASE_RESERVED_CHARS.has(ch)) {
                    throw new Error(`Base digit '${ch}' is reserved and cannot be used in a digit alphabet`);
                }
            }
            return baseSystem;
        };
        if (baseSpecValue && baseSpecValue.type === "string") {
            const digits = baseSpecValue.value;
            return ensureReservedChars(new BaseSystem(Array.from(digits), `Custom Base ${Array.from(digits).length}`));
        }
        if (typeof baseSpecValue === "string") {
            return ensureReservedChars(new BaseSystem(Array.from(baseSpecValue), `Custom Base ${Array.from(baseSpecValue).length}`));
        }
        if (baseSpecValue instanceof Integer || baseSpecValue instanceof Rational) {
            const n = baseSpecValue instanceof Integer ? baseSpecValue.value : baseSpecValue.numerator;
            const d = baseSpecValue instanceof Rational ? baseSpecValue.denominator : 1n;
            if (d !== 1n) throw new Error("Base number must be an integer");
            const baseNum = Number(n);
            if (!Number.isInteger(baseNum) || baseNum < 2 || baseNum > 64) {
                throw new Error("Base number must be an integer between 2 and 64");
            }
            return ensureReservedChars(BaseSystem.fromBase(baseNum));
        }
        throw new Error("Invalid base specification");
    }

    _resolveBaseSpecText(baseSpecText, scopeChain = [new Map()]) {
        const spec = baseSpecText.trim();
        const prefixMatch = spec.match(/^0([A-Za-z])$/);
        if (prefixMatch) {
            const bs = BaseSystem.getSystemForPrefix(prefixMatch[1]);
            if (!bs) throw new Error(`Unknown base prefix '0${prefixMatch[1]}'`);
            return bs;
        }
        if (spec.startsWith('"') && spec.endsWith('"')) {
            return this._resolveBaseSpecValue({ type: "string", value: this._stringLiteralValue(spec) });
        }
        const tupleMatch = spec.match(/^\{\:\s*([^,]+)\s*,\s*("(?:(?:[^"\\]|\\.)*)")\s*\}$/);
        if (tupleMatch) {
            const radixEval = this.evaluateExpression(tupleMatch[1], scopeChain);
            if (radixEval.type === "error") throw new Error(radixEval.message);
            const digits = this._stringLiteralValue(tupleMatch[2]);
            const base = this._resolveBaseSpecValue(radixEval.result);
            const radix = base.base;
            const cpLen = Array.from(digits).length;
            if (radix !== cpLen) {
                throw new Error(`Tuple base mismatch: radix ${radix} does not match digits length ${cpLen}`);
            }
            return this._resolveBaseSpecValue({ type: "string", value: digits });
        }
        const evalRes = this.evaluateExpression(spec, scopeChain);
        if (evalRes.type === "error") throw new Error(evalRes.message);
        return this._resolveBaseSpecValue(evalRes.result);
    }

    _resolveModeValue(modeValue) {
        if (modeValue === undefined || modeValue === null) return 1;
        if (modeValue && modeValue.type === "string") {
            const mapped = VariableManager.BASE_MODE_ALIASES.get(modeValue.value.trim().toLowerCase());
            if (!mapped) throw new Error(`Unknown formatting mode '${modeValue.value}'`);
            return mapped;
        }
        if (typeof modeValue === "string") {
            const mapped = VariableManager.BASE_MODE_ALIASES.get(modeValue.trim().toLowerCase());
            if (!mapped) throw new Error(`Unknown formatting mode '${modeValue}'`);
            return mapped;
        }
        if (modeValue instanceof Integer) return Number(modeValue.value);
        if (modeValue instanceof Rational && modeValue.denominator === 1n) return Number(modeValue.numerator);
        throw new Error("Formatting mode must be an integer or mode string");
    }

    _toBaseIntegerDigits(value, baseSystem) {
        return this._groupDigits(baseSystem.fromDecimal(value));
    }

    _formatToBase(value, baseSystem, mode = 1) {
        const rat = this._toRationalValue(value);
        if (mode === 2) {
            const raw = rat.toRepeatingBase(baseSystem);
            const shortened = this._shortenRepeatingExpansion(raw);
            return this._groupRadixExpansion(shortened, baseSystem);
        }
        if (mode === 3 || mode === 4) {
            const cf = rat.toContinuedFraction();
            const terms = cf.map((t) => baseSystem.fromDecimal(t));
            if (terms.length === 1) return this._groupDigits(terms[0]);
            const prefix = (mode === 4 || cf[0] < 0n) ? "~" : "";
            return `${prefix}${this._groupDigits(terms[0])}.~${terms.slice(1).map((t) => this._groupDigits(t)).join("~")}`;
        }
        if (mode === 5) {
            const raw = this._shortenRepeatingExpansion(rat.toRepeatingBase(baseSystem));
            const sign = raw.startsWith("-") ? "-" : "";
            const body = sign ? raw.substring(1) : raw;
            const dot = body.indexOf(".");
            const hash = body.indexOf("#");
            const integer = dot === -1 ? (hash === -1 ? body : body.substring(0, hash)) : body.substring(0, dot);
            const integerDigits = Array.from(integer).filter((ch) => baseSystem.charMap.has(ch)).length;
            if (integer.length <= 1 && integer !== "0") {
                return `${this._groupRadixExpansion(sign + body, baseSystem)}_^0`;
            }
            if (integer.length > 1) {
                const shifted = `${integer[0]}.${integer.substring(1)}${dot === -1 ? (hash === -1 ? "" : body.substring(hash)) : body.substring(dot + 1)}`;
                const groupedShifted = this._groupRadixExpansion(sign + shifted, baseSystem);
                return `${groupedShifted}_^${integerDigits - 1}`;
            }
            return `${this._groupRadixExpansion(sign + body, baseSystem)}_^0`;
        }
        if (mode === 6) {
            return `${this._toBaseIntegerDigits(rat.numerator, baseSystem)}/${this._toBaseIntegerDigits(rat.denominator, baseSystem)}`;
        }
        if (rat.denominator === 1n) {
            return this._toBaseIntegerDigits(rat.numerator, baseSystem);
        }
        const sign = rat.numerator < 0n ? -1n : 1n;
        const absNum = rat.numerator < 0n ? -rat.numerator : rat.numerator;
        const whole = absNum / rat.denominator;
        const rem = absNum % rat.denominator;
        if (rem === 0n) return this._toBaseIntegerDigits(rat.numerator, baseSystem);
        const wholeStr = this._toBaseIntegerDigits(sign < 0n ? -whole : whole, baseSystem);
        const fracNum = this._toBaseIntegerDigits(rem, baseSystem);
        const fracDen = this._toBaseIntegerDigits(rat.denominator, baseSystem);
        return `${wholeStr}..${fracNum}/${fracDen}`;
    }

    _splitTopLevelBaseOp(expr, op) {
        let depthParen = 0;
        let depthBracket = 0;
        let depthBrace = 0;
        let inString = false;
        for (let i = 0; i <= expr.length - op.length; i++) {
            const ch = expr[i];
            if (ch === '"') {
                let bs = 0;
                let j = i - 1;
                while (j >= 0 && expr[j] === "\\") {
                    bs++;
                    j--;
                }
                if (bs % 2 === 0) inString = !inString;
            }
            if (inString) continue;
            if (ch === "(") depthParen++;
            else if (ch === ")") depthParen--;
            else if (ch === "[") depthBracket++;
            else if (ch === "]") depthBracket--;
            else if (ch === "{") depthBrace++;
            else if (ch === "}") depthBrace--;
            if (depthParen === 0 && depthBracket === 0 && depthBrace === 0 && expr.startsWith(op, i)) {
                return { left: expr.substring(0, i).trim(), right: expr.substring(i + op.length).trim() };
            }
        }
        return null;
    }

    _evaluateBaseOperators(expression, scopeChain = [new Map()]) {
        const splitTupleParts = (text) => {
            const s = text.trim();
            if (!(s.startsWith("(") && s.endsWith(")"))) return null;
            const inner = s.substring(1, s.length - 1);
            let depthParen = 0;
            let depthBracket = 0;
            let depthBrace = 0;
            let inString = false;
            for (let i = 0; i < inner.length; i++) {
                const ch = inner[i];
                if (ch === '"') {
                    let bs = 0;
                    let j = i - 1;
                    while (j >= 0 && inner[j] === "\\") {
                        bs++;
                        j--;
                    }
                    if (bs % 2 === 0) inString = !inString;
                    continue;
                }
                if (inString) continue;
                if (ch === "(") depthParen++;
                else if (ch === ")") depthParen--;
                else if (ch === "[") depthBracket++;
                else if (ch === "]") depthBracket--;
                else if (ch === "{") depthBrace++;
                else if (ch === "}") depthBrace--;
                else if (ch === "," && depthParen === 0 && depthBracket === 0 && depthBrace === 0) {
                    const left = inner.substring(0, i).trim();
                    const right = inner.substring(i + 1).trim();
                    if (!left || !right) return null;
                    return { left, right };
                }
            }
            return null;
        };

        const parseModeOrSpec = (specText) => {
            const tuple = splitTupleParts(specText);
            if (tuple) {
                return { baseSpec: tuple.left, modeExpr: tuple.right };
            }
            if (specText.startsWith('"') && specText.endsWith('"')) {
                const modeMaybe = this._stringLiteralValue(specText).trim().toLowerCase();
                if (VariableManager.BASE_MODE_ALIASES.has(modeMaybe)) {
                    return { baseSpec: "10", modeExpr: specText };
                }
            }
            return { baseSpec: specText, modeExpr: null };
        };

        const fromSplit = this._splitTopLevelBaseOp(expression, "<_");
        if (fromSplit) {
            const leftEval = this.evaluateExpression(fromSplit.left, scopeChain);
            if (leftEval.type === "error") throw new Error(leftEval.message);
            const input = leftEval.result?.type === "string" ? leftEval.result.value : leftEval.result;
            if (typeof input !== "string") throw new Error("<_ expects a string on the left");
            const baseSystem = this._resolveBaseSpecText(fromSplit.right, scopeChain);
            return this.fromBaseString(input, baseSystem);
        }

        const toSplit = this._splitTopLevelBaseOp(expression, "_>");
        if (toSplit) {
            const leftEval = this.evaluateExpression(toSplit.left, scopeChain);
            if (leftEval.type === "error") throw new Error(leftEval.message);
            const { baseSpec, modeExpr } = parseModeOrSpec(toSplit.right);
            const baseSystem = this._resolveBaseSpecText(baseSpec, scopeChain);
            let mode = 1;
            if (modeExpr) {
                const modeEval = this.evaluateExpression(modeExpr, scopeChain);
                if (modeEval.type === "error") throw new Error(modeEval.message);
                mode = this._resolveModeValue(modeEval.result);
            }
            const baseStr = this._formatToBase(leftEval.result, baseSystem, mode);
            return { type: "string", value: baseStr };
        }

        return null;
    }

    _normalizePrefixedBaseLiterals(expression) {
        let out = "";
        let i = 0;
        while (i < expression.length) {
            const ch = expression[i];
            if (ch === '"' || (ch === "0" && i + 1 < expression.length && /[A-Z]/.test(expression[i + 1]))) {
                if (ch === '"') {
                    out += ch;
                    i++;
                    while (i < expression.length) {
                        out += expression[i];
                        if (expression[i] === '"') {
                            let bs = 0;
                            let j = i - 1;
                            while (j >= 0 && expression[j] === "\\") {
                                bs++;
                                j--;
                            }
                            if (bs % 2 === 0) {
                                i++;
                                break;
                            }
                        }
                        i++;
                    }
                    continue;
                }
                const prefix = expression[i + 1];
                const bs = BaseSystem.getSystemForPrefix(prefix);
                if (!bs) {
                    out += ch;
                    i++;
                    continue;
                }
                let j = i + 2;
                let digitStream = "";
                if (j < expression.length && expression[j] === '"') {
                    let k = j + 1;
                    let raw = "";
                    let closed = false;
                    while (k < expression.length) {
                        if (expression[k] === '"') {
                            let esc = 0;
                            let p = k - 1;
                            while (p >= j + 1 && expression[p] === "\\") {
                                esc++;
                                p--;
                            }
                            if (esc % 2 === 0) {
                                closed = true;
                                break;
                            }
                        }
                        raw += expression[k];
                        k++;
                    }
                    if (closed) {
                        digitStream = raw
                            .replace(/\\u([0-9a-fA-F]{4})/g, (_, hex) => String.fromCharCode(parseInt(hex, 16)))
                            .replace(/\\"/g, '"')
                            .replace(/\\\\/g, "\\");
                        j = k + 1;
                    }
                } else {
                    while (j < expression.length && VariableManager.SAFE_UNQUOTED_BASE_CHARS.test(expression[j])) {
                        digitStream += expression[j];
                        j++;
                    }
                }
                if (!digitStream.length) {
                    out += ch;
                    i++;
                    continue;
                }
                const parsed = this.fromBaseString(digitStream, bs);
                out += this.formatValueWithPrefix(parsed);
                i = j;
                continue;
            }
            out += ch;
            i++;
        }
        return out;
    }

    defineBasePrefix(prefixLetter, rhsExpression) {
        if (!/^[A-Z]$/.test(prefixLetter)) {
            throw new Error("Base definitions require an uppercase prefix letter");
        }
        const hasExactPrefix = typeof BaseSystem.hasExactPrefix === "function"
            ? BaseSystem.hasExactPrefix(prefixLetter)
            : !!BaseSystem.getSystemForPrefix(prefixLetter);
        if (hasExactPrefix) {
            throw new Error(`Base prefix 0${prefixLetter} is already defined`);
        }
        const baseSystem = this._resolveBaseSpecText(rhsExpression, [new Map()]);
        for (const ch of baseSystem.characters) {
            if (VariableManager.BASE_RESERVED_CHARS.has(ch)) {
                throw new Error(`Base digit '${ch}' is reserved and cannot be used in a digit alphabet`);
            }
        }
        BaseSystem.registerPrefix(prefixLetter, baseSystem);
        this.customBases.set(prefixLetter, baseSystem);
        return new Integer(1n);
    }

    /**
     * Preprocess expression to convert numbers from input base to decimal
     * Only converts bare numbers, preserves explicit base notation like 101[2]
     * @param {string} expression - The expression to preprocess
     * @returns {string} - The preprocessed expression with numbers converted to decimal
     */
    preprocessExpression(expression) {
        if (!this.inputBase || this.inputBase.base === 10) {
            return expression; // No conversion needed for decimal base
        }

        // Create a character class pattern for valid characters in this base
        // For bases > 10, include both uppercase and lowercase letters
        let validChars = this.inputBase.characters
            .map((c) =>
                // Escape special regex characters
                c.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"),
            )
            .join("");

        // Add uppercase versions of letters for bases > 10
        if (this.inputBase.base > 10) {
            const uppercaseChars = this.inputBase.characters
                .filter((c) => /[a-z]/.test(c))
                .map((c) => c.toUpperCase())
                .map((c) => c.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"))
                .join("");
            validChars += uppercaseChars;
        }

        // Regular expression to match bare numbers (not followed by [base])
        // Uses the valid characters for this specific base
        // Also captures uncertainty notation like 1.23[45:67] to avoid misidentifying parts of it
        const numberPattern = new RegExp(
            `\\b(?:-?0z\\[\\d+\\])?(-?[${validChars}0-9a-zA-Z]+(?:\\.[${validChars}0-9a-zA-Z]*)?(?:\\.\\.[${validChars}0-9a-zA-Z]+(?:\\/[${validChars}0-9a-zA-Z]+)?)?(?:\\/[${validChars}0-9a-zA-Z]+)?(?:\\_\\^-?[${validChars}0-9a-zA-Z]+)?)(?:\\[([^\\]]+)\\](?:[Ee][+-]?[${validChars}0-9a-zA-Z]+|\\_\\^-?[${validChars}0-9a-zA-Z]+)?|\\b(?!\\s*\\[))`,
            "g",
        );

        // Process expression preserving strings
        let result = "";
        let i = 0;
        let chunkStart = 0;
        let inString = false;

        while (i < expression.length) {
            if (expression[i] === '"') {
                if (!inString) {
                    // Start of string. Process previous chunk.
                    const chunk = expression.substring(chunkStart, i);
                    result += this._processChunk(chunk, numberPattern);
                    inString = true;
                    result += '"'; // append quote
                } else {
                    // Check for escaped quote
                    let backslashCount = 0;
                    let j = i - 1;
                    while (j >= chunkStart && expression[j] === '\\') {
                        backslashCount++;
                        j--;
                    }
                    result += '"'; // append quote
                    if (backslashCount % 2 === 0) {
                        // End of string
                        inString = false;
                        chunkStart = i + 1;
                    }
                }
            } else if (inString) {
                result += expression[i];
            }
            i++;
        }

        // Process remaining chunk
        if (!inString && chunkStart < expression.length) {
            const chunk = expression.substring(chunkStart);
            result += this._processChunk(chunk, numberPattern);
        } else if (inString) {
            // Unterminated string - just append (Parser will error later)
            // Or technically it's part of the string.
        }

        return result;
    }

    _processChunk(chunk, numberPattern) {
        return chunk.replace(numberPattern, (match, baseValue, uncertainty) => {
            if (uncertainty) {
                // If it's uncertainty notation, return as is (Parser will handle it)
                return match;
            }
            try {
                // Normalize for case-insensitive bases (letters) i.e. Base <= 36
                const normalize = (s) =>
                    this.inputBase.base <= 36 && this.inputBase.base > 10
                        ? s.toLowerCase()
                        : s;

                // Skip processing if it looks like a prefix notation (e.g., 0b..., 0x...)
                // Strict prefix handling requires this to be passed to Parser
                // Exception: if it is 0d, we might want to ensure it is handled? 
                // But Parser handles prefixes fine.
                if (/^-?0[a-zA-Z]/.test(match)) {
                    return match;
                }

                // Helper to parse a standard number string (int, decimal, fraction) to Rational
                const parseToRational = (str) => {
                    // Handle mixed numbers: whole..num/den
                    if (str.includes("..")) {
                        const [whole, fraction] = str.split("..");
                        const wholeDec = this.inputBase.toDecimal(normalize(whole));
                        if (fraction.includes("/")) {
                            const [num, den] = fraction.split("/");
                            const numDec = this.inputBase.toDecimal(normalize(num));
                            const denDec = this.inputBase.toDecimal(normalize(den));
                            // whole + num/den
                            const w = new Rational(wholeDec);
                            const n = new Rational(numDec);
                            const d = new Rational(denDec);
                            return w.add(n.divide(d));
                        } else {
                            const fracDec = this.inputBase.toDecimal(normalize(fraction));
                            // Ambiguous: whole..frac? usually means whole + frac/base^len
                            // But original code returned string.
                            // We need Value.
                            // Re-implement logic: 'whole' numeric + 'fraction' numeric?
                            // No, typically whole..num is mixed fraction.
                            // Original code line 85: wholeDec..fracDec.
                            // It returned STRING. Parser parsed it.
                            // If we want Value, we must interpret it.
                            // Assuming RatMath parser handles `whole..frac` as Mixed.
                            // Let's defer to original logic: parse to string, then wrap in 0d?
                            // But `0d3..4` is valid?
                            // Yes if Parser handles it. 
                            // BUT `0d` enforces Decimal. `3..4` in Decimal is 3 + 4/10?
                            // Original logic converted parts to Decimal.
                            // So `a..b` (Hex) -> `10..11`.
                            // `0d10..11`. Parser parses `10` mixed `11`.
                            // `10 + 11/100`?
                            // Is that correct meaning?
                            // Probably.

                            // Let's stick to parsing segments to decimal strings and reconstructing the string.
                            return `${wholeDec}..${fracDec}`;
                        }
                    }

                    // Handle simple fractions: num/den
                    if (str.includes("/") && !str.includes(".")) {
                        const [num, den] = str.split("/");
                        const numDec = this.inputBase.toDecimal(normalize(num));
                        const denDec = this.inputBase.toDecimal(normalize(den));
                        return `${numDec}/${denDec}`;
                    }

                    // Handle decimals: int.frac
                    if (str.includes(".")) {
                        const [intStr, fracStr] = str.split(".");
                        const isNegative = intStr.startsWith("-");

                        // Use existing logic to convert decimal to Rational string
                        let val = new Rational(this.inputBase.toDecimal(normalize(intStr)));
                        const base = BigInt(this.inputBase.base);
                        let divisor = base;

                        for (const char of fracStr) {
                            const digitValue = this.inputBase.toDecimal(normalize(char));
                            const term = new Rational(digitValue, divisor);
                            val = isNegative ? val.subtract(term) : val.add(term);
                            divisor *= base;
                        }
                        return val.toString();
                    }

                    // Handle simple integers
                    let targetStr = str;
                    let isNeg = false;
                    if (targetStr.startsWith("-")) {
                        isNeg = true;
                        targetStr = targetStr.substring(1);
                    }
                    const val = this.inputBase.toDecimal(normalize(targetStr));
                    return isNeg ? (-val).toString() : val.toString();
                };

                // Helper to format with 0z[10] prefix handling negatives
                const toPrefixedDecimal = (s) => s.startsWith("-") ? `-0z[10]${s.substring(1)}` : `0z[10]${s}`;

                // Check for Scientific Notation _^
                if (match.includes("_^")) {
                    const [basePart, expPart] = match.split("_^");
                    const baseValStr = parseToRational(basePart);
                    const expValStr = parseToRational(expPart);
                    // Construct expression: (Base) * (SystemBase) ^ (Exp)
                    return `(${toPrefixedDecimal(baseValStr)}) * (${toPrefixedDecimal(this.inputBase.base.toString())}) ^ (${toPrefixedDecimal(expValStr)})`;
                }

                // Standard Number
                const valStr = parseToRational(match);
                // Return with 0z[10] prefix to strip context
                return toPrefixedDecimal(valStr);

            } catch (error) {
                // If conversion fails for any part, return as-is
                return match;
            }
        });
    }

    /**
     * Parse and process an input that may contain assignments or function definitions
     * @param {string} input - The input string
     * @returns {object} - {type: 'assignment'|'function'|'expression', result: any, message: string}
     */
    processInput(input) {
        try {
            const trimmed = input.trim();

            // Base definition assignment: 0A = "..." or 0A := {: 16, "..."}
            const baseDefMatch = trimmed.match(/^0([A-Z])\s*(?::=|=)\s*(.+)$/);
            if (baseDefMatch) {
                const [, prefixLetter, rhs] = baseDefMatch;
                const result = this.defineBasePrefix(prefixLetter, rhs);
                return {
                    type: "assignment",
                    result,
                    message: `Defined base prefix 0${prefixLetter}`,
                };
            }

            // 1. Function Definition: Name(args) -> body
            // Matches: FuncName(args) -> body
            // FuncName must be generic word or @word to match properly (validation inside handler)
            // IMPORTANT: params must not contain '(' to avoid matching function calls with lambda args
            // e.g., Map(L, (x,i)->x^2) should NOT match as function definition
            const funcDefMatch = trimmed.match(/^(@?[_a-zA-Z][a-zA-Z0-9_]*)\s*\(([^()]*)\)\s*->\s*(.+)$/);
            if (funcDefMatch) {
                const [, funcName, paramStr, body] = funcDefMatch;
                const rawParams = paramStr.split(",").map(p => p.trim()).filter(p => p);

                const params = [];
                const defaults = {};

                for (const p of rawParams) {
                    if (p.includes('?')) {
                        const [pName, pDef] = p.split('?').map(s => s.trim());
                        if (!pName) throw new Error("Invalid parameter syntax: " + p);
                        params.push(pName + '?'); // Mark as optional for signature consistency
                        if (pDef) defaults[pName] = pDef;
                    } else {
                        params.push(p);
                    }
                }

                return this.handleFunctionDefinition(funcName, params, body, undefined, defaults);
            }

            // 2. Assignment Style Definition: Name = (args) -> body  OR  Name = arg -> body
            // Matches: Name = ... -> ...
            const arrowAssignMatch = trimmed.match(/^(@?[_a-zA-Z][a-zA-Z0-9_]*)\s*=\s*(?:(?:\(([^)]*)\))|([_a-zA-Z][a-zA-Z0-9_]*))\s*->\s*(.+)$/);
            if (arrowAssignMatch) {
                const [, name, paramsInParens, singleParam, body] = arrowAssignMatch;
                const rawParams = (paramsInParens !== undefined ? paramsInParens : singleParam)
                    .split(",").map(p => p.trim()).filter(p => p);

                const params = [];
                const defaults = {};

                for (const p of rawParams) {
                    if (p.includes('?')) {
                        const [pName, pDef] = p.split('?').map(s => s.trim());
                        if (!pName) throw new Error("Invalid parameter syntax: " + p);
                        params.push(pName + '?');
                        if (pDef) defaults[pName] = pDef;
                    } else {
                        params.push(p);
                    }
                }

                // This is a function definition disguised as assignment
                return this.handleFunctionDefinition(name, params, body, undefined, defaults);
            }

            // 3. Property Assignment: Name.property = Expression
            // Matches: P.type = "poly", P.Derivative = SomeFunc, etc.
            const propAssignMatch = trimmed.match(/^(@?[_a-zA-Z][a-zA-Z0-9_]*)\.([_a-zA-Z][a-zA-Z0-9_]*)\s*=\s*(.+)$/);
            if (propAssignMatch) {
                const [, targetName, propName, expression] = propAssignMatch;
                return this.handlePropertyAssignment(targetName, propName, expression);
            }

            // 4. Variable Assignment: Name = Expression
            // Matches: Name = ... (but NOT -> as that's handled above)
            // We match generic identifier, validation of case happens in handleAssignment
            const assignmentMatch = trimmed.match(/^(@?[_a-zA-Z][a-zA-Z0-9_]*)\s*=\s*(.+)$/);
            if (assignmentMatch) {
                const [, varName, expression] = assignmentMatch;
                return this.handleAssignment(varName, expression);
            }

            // 4. Old bracket syntax (P[x,y] = expr) - Deprecate or Keep? 
            // Keeping for backward compat if desired, but user didn't ask. 
            // Let's rely on new syntax primarily.

            // 5. Special Functions (SUM/PROD)
            if (/^(SUM|PROD|SEQ)/.test(trimmed)) {
                // Fallback to evaluateExpression which handles these
            }

            // 6. Function Display / Inspection: Name or @Name
            // If user types just a function name, return its definition.
            const funcLookupMatch = trimmed.match(/^(@?[A-Z][a-zA-Z0-9]*)$/);
            if (funcLookupMatch) {
                const name = funcLookupMatch[1];
                // Normalize: strip @
                const normalizedName = this.normalizeName(name.startsWith("@") ? name.substring(1) : name);

                if (this.functions.has(normalizedName)) {
                    // Ambiguity Check
                    let isDigit = false;
                    if (this.inputBase) {
                        const validChars = this.inputBase.base > 10
                            ? this.inputBase.characters.concat(this.inputBase.characters.filter(c => /[a-z]/.test(c)).map(c => c.toUpperCase()))
                            : this.inputBase.characters;
                        // Check entire string 'name' (without @) against valid chars? 
                        // Wait, 'name' in input might have @. 
                        // Digits strictly don't have @.
                        // So we check name WITHOUT @ if user typed it WITHOUT @.
                        // If user typed @A, it's not a digit.
                        // If user typed A, it might be.
                        if (!name.startsWith("@")) {
                            isDigit = [...name].every(c => validChars.includes(c));
                        }
                    }

                    if (isDigit) {
                        // Ambiguous! Function vs Digit.
                        throw new Error(`Ambiguous reference '${name}'. Use @${name} for function or explicit base prefix (e.g. 0z[10]${name} or 0x${name}) for number.`);
                    }

                    const f = this.functions.get(normalizedName);

                    // Check for _display property for custom display
                    const displayProp = this.getDecoration(normalizedName, "_display");
                    if (displayProp) {
                        // If _display is a function, call it; otherwise use as string
                        let displayStr;
                        if (typeof displayProp === 'string' && this.functions.has(displayProp)) {
                            // It's a function reference, call it
                            try {
                                const dispResult = this.evaluateExpression(`${displayProp}()`);
                                displayStr = dispResult.type !== 'error' ? this.formatValue(dispResult.result) : displayProp;
                            } catch {
                                displayStr = displayProp;
                            }
                        } else if (displayProp.type === 'string') {
                            displayStr = displayProp.value;
                        } else {
                            displayStr = this.formatValue(displayProp);
                        }
                        return {
                            type: "function_display",
                            result: displayStr,
                            message: displayStr
                        };
                    }

                    // Default display
                    return {
                        type: "function_display",
                        result: `${normalizedName}(${f.params.join(", ")}) -> ${f.body}`,
                        message: `${normalizedName}(${f.params.join(", ")}) -> ${f.body}`
                    };
                }
            }

            // Fallback: Evaluate as expression

            // Fallback: Evaluate as expression
            // Fallback: Evaluate as expression
            // Fallback: Evaluate as expression
            // Use this.variables as the top-level scope so that functions like ASSIGN work persistently
            return this.evaluateExpression(trimmed, [this.variables]);
        } catch (error) {
            return {
                type: "error",
                message: error.message
            };
        }
    }

    /**
     * Handle variable assignment
     */
    handleAssignment(varName, expression) {
        // Check case of varName
        const isUpperCase = /^[A-Z][a-zA-Z0-9]*$/.test(varName) || (varName.startsWith("@") && /^[A-Z]/.test(varName.substring(1)));

        if (isUpperCase) {
            // 1. Strict Assignment: Uppercase names are functions.
            // Check for Aliasing first
            const aliasMatch = expression.trim().match(/^(@?[A-Z][a-zA-Z0-9]*)$/);
            if (aliasMatch) {
                const sourceName = aliasMatch[1];
                const normSource = this.normalizeName(sourceName.startsWith("@") ? sourceName.substring(1) : sourceName);
                const normTarget = this.normalizeName(varName.startsWith("@") ? varName.substring(1) : varName);

                if (this.functions.has(normSource)) {
                    const sourceDef = this.functions.get(normSource);
                    this.functions.set(normTarget, { ...sourceDef });
                    return {
                        type: "function",
                        result: null,
                        message: `Function ${normTarget} defined as alias of ${normSource}`
                    };
                }
            }

            // 2. Check for List/Sequence or Object Assignment
            // Evaluate expression to see if it's a list or object
            try {
                const result = this.evaluateExpression(expression);
                if (result.type !== "error" && result.result) {
                    const normTarget = this.normalizeName(varName.startsWith("@") ? varName.substring(1) : varName);

                    if (result.result.type === "sequence") {
                        // Capital names: Store as List Accessor Function with L(i) syntax
                        // L(i) -> element
                        // L(0) -> full list
                        // L(-i) -> from end
                        this.functions.set(normTarget, {
                            type: 'list_accessor',
                            list: result.result,
                            params: ['index'],
                            doc: `List Accessor for ${this.formatValue(result.result)}`
                        });

                        return {
                            type: "function",
                            result: result.result,
                            message: `List Accessor ${normTarget} defined. ${normTarget}(i) to access elements.`
                        };
                    }

                    if (result.result.type === "object") {
                        // Object assignment: P = {a=5, b=10, _eval=x->...}
                        // Create an "object" function that stores properties
                        const obj = result.result;

                        // Check for special properties
                        const hasEval = obj.properties.has("_eval");
                        const hasDisplay = obj.properties.has("_display");
                        const hasDefinition = obj.properties.has("_definition");

                        // Create function definition based on _eval or default identity
                        if (hasEval) {
                            const evalFunc = obj.properties.get("_eval");
                            // If _eval is a lambda reference, use its definition
                            if (typeof evalFunc === 'string' && this.functions.has(evalFunc)) {
                                const funcDef = this.functions.get(evalFunc);
                                this.functions.set(normTarget, {
                                    ...funcDef,
                                    type: 'object_function',
                                    objectProps: obj.properties
                                });
                            } else {
                                // Store as object with eval property
                                this.functions.set(normTarget, {
                                    type: 'object_function',
                                    params: ['x'],
                                    body: '_eval(x)',
                                    objectProps: obj.properties,
                                    doc: `Object ${normTarget}`
                                });
                            }
                        } else {
                            // No _eval, just store as object holder
                            this.functions.set(normTarget, {
                                type: 'object_function',
                                params: [],
                                objectProps: obj.properties,
                                doc: `Object ${normTarget}`
                            });
                        }

                        // Copy all properties as decorations
                        for (const [key, value] of obj.properties) {
                            this.setDecoration(normTarget, key, value);
                        }

                        const propCount = obj.properties.size;
                        return {
                            type: "function",
                            result: obj,
                            message: `Object ${normTarget} defined with ${propCount} properties.`
                        };
                    }
                }
            } catch (e) {
                // Ignore eval errors, fall through to error message
            }

            // If not a valid alias or list, reject assignment
            return {
                type: "error",
                message: `Function names (starting with Uppercase) cannot be assigned values directly (unless it is a List). To define a function, use '->' syntax or alias an existing function.`
            };
        }

        // Lowercase Variable Assignment
        try {
            const result = this.evaluateExpression(expression);

            if (result.type === "error") {
                return result;
            }

            // For lowercase names, store sequences and objects as raw values (not accessors)
            let valueToStore = result.result;
            let displayValue = result.result;

            // Sequences with lastValue (like a,b,c) store just the last value
            // But explicit list literals [1,2,3] are stored as raw sequences
            if (result.result && result.result.type === "sequence") {
                if (result.result.lastValue !== undefined) {
                    // Comma-separated values - store last value
                    valueToStore = result.result.lastValue;
                }
                // Otherwise store the full sequence as raw value
            }
            // Objects are stored directly as raw object values

            const normalizedVarName = this.normalizeName(varName);

            // Handle Promise results - return async assignment that caller can await
            if (valueToStore && typeof valueToStore.then === 'function') {
                return {
                    type: "async_assignment",
                    varName: normalizedVarName,
                    promise: valueToStore,
                    variableManager: this
                };
            }

            this.variables.set(normalizedVarName, valueToStore);

            // Format message
            let message = `${normalizedVarName} = ${this.formatValue(valueToStore)}`;

            return {
                type: "assignment",
                result: valueToStore,
                message: message,
            };
        } catch (error) {
            return {
                type: "error",
                message: `Assignment error: ${error.message}`,
            };
        }
    }

    /**
     * Handle property assignment (P.type = value)
     */
    handlePropertyAssignment(targetName, propName, expression) {
        try {
            // Normalize target name with case normalization
            const normalizedTarget = this.normalizeName(targetName.startsWith("@") ? targetName.substring(1) : targetName);

            // Check if target exists (variable or function)
            const targetExists = this.variables.has(normalizedTarget) || this.functions.has(normalizedTarget);
            if (!targetExists) {
                return {
                    type: "error",
                    message: `Cannot set property on undefined target '${normalizedTarget}'. Define the variable or function first.`
                };
            }

            // Evaluate the expression
            const result = this.evaluateExpression(expression);
            if (result.type === "error") {
                return result;
            }

            // Wrap raw strings as string objects for consistency
            let valueToStore = result.result;
            if (typeof valueToStore === 'string') {
                valueToStore = { type: 'string', value: valueToStore };
            }

            // Set the decoration
            this.setDecoration(normalizedTarget, propName, valueToStore);

            // Return the formatted value as a string for better display
            const formattedValue = this.formatValue(valueToStore);

            return {
                type: "property_assignment",
                result: formattedValue,
                message: `${normalizedTarget}.${propName} = ${formattedValue}`
            };
        } catch (error) {
            return {
                type: "error",
                message: `Property assignment error: ${error.message}`
            };
        }
    }

    /**
     * Handle function definition
     */
    handleFunctionDefinition(funcName, params, body, doc, defaults = {}) {
        // Validate parameters
        if (params.length === 0) {
            // 0 params ok
        }
        // Check duplicates
        if (new Set(params).size !== params.length) {
            return { type: "error", message: "Duplicate parameter names" };
        }

        const paramSet = new Set();
        // Check format
        for (const param of params) {
            // Allow param? 
            const clean = param.replace(/\?$/, '');
            if (!/^[a-zA-Z][a-zA-Z0-9_]*$/.test(clean)) {
                return {
                    type: "error",
                    message: `Invalid parameter name '${param}'. Must start with letter.`
                };
            }
            paramSet.add(clean);

            // Ambiguity Check: Is this parameter name a valid number in current base?
            // e.g. 'a' in HEX is 10.
            try {
                const res = this.evaluateExpression(clean, new Map());
                // If it succeeds and returns a value without variable lookups...
                if (res.type !== 'error' && res.result !== undefined) {
                    // It parsed as a number!
                    return {
                        type: "error",
                        message: `Ambiguous parameter '${clean}'. It is a valid number in the current base (${this.inputBase}).`
                    };
                }
            } catch (e) {
                // Not a number, good.
            }
        }

        // STATIC SCOPING & BASE SAFETY
        // 1. Numbers: Convert to 0z[10] decimal literal to make them base-independent
        // 2. Variables: If not in params and not underscore prefixed, capture current value (freeze)
        // 3. Defaults: Freeze default values too

        const staticBody = this.freezeExpression(body, paramSet);

        // Freeze Defaults
        const staticDefaults = {};
        for (const [key, val] of Object.entries(defaults)) {
            staticDefaults[key] = this.freezeExpression(val, paramSet);
        }

        const normalizedFuncName = this.normalizeName(funcName);
        this.functions.set(normalizedFuncName, { params, body: staticBody, type: 'def', doc: doc || `User defined function: ${body}`, defaults: staticDefaults });
        return {
            type: "function",
            result: null,
            message: `Function ${normalizedFuncName}[${params.join(",")}] defined`,
        };
    }

    /**
     * Handle function call
     */
    handleFunctionCall(funcName, argsStr) {
        const normalizedFuncName = this.normalizeName(funcName);
        if (!this.functions.has(normalizedFuncName)) {
            return {
                type: "error",
                message: `Function ${funcName} not defined`,
            };
        }

        const func = this.functions.get(normalizedFuncName);

        // JS Function Handler shortcut
        if (func.type === 'js') {
            // Still need to parse args!
            // We reuse the parsing logic below.
        }

        // Basic arg splitting - need to be smarter to respect parens if not already?
        // Actually handleFunctionCall receives argsStr which is inner parens.
        // We need to split by comma, respecting sub-balanced-parens/brackets.

        const args = [];
        let currentArg = "";
        let depth = 0;
        for (let i = 0; i < argsStr.length; i++) {
            const char = argsStr[i];
            if (char === '(' || char === '[' || char === '{') depth++;
            else if (char === ')' || char === ']' || char === '}') depth--;

            if (char === ',' && depth === 0) {
                // Push trimmed arg, OR undefined if empty (skipping)
                const trimmed = currentArg.trim();
                args.push(trimmed === "" ? undefined : trimmed);
                currentArg = "";
            } else {
                currentArg += char;
            }
        }
        // Push last arg if not empty string
        const lastTrimmed = currentArg.trim();
        if (lastTrimmed !== "") {
            args.push(lastTrimmed);
        }

        // Calculate min required args based on optional params (ending with ?)
        const minArgs = func.params.filter(p => !p.endsWith('?')).length;
        const maxArgs = func.params.length;

        // Check argument count (allow less than min if defaults exist? No, defaults are for optionals usually)
        // If an argument is undefined (skipped), we check if there is a default later.
        if (args.length > maxArgs) {
            return {
                type: "error",
                message: `Function ${funcName} expects at most ${maxArgs} arguments, got ${args.length}`,
            };
        }

        try {
            // Evaluate arguments with Strict Typing and Lambda support
            const argValues = [];

            // Loop up to maxArgs (params length) to handle defaults
            for (let i = 0; i < maxArgs; i++) {
                let argRaw = args[i]; // May be undefined (skipped or missing at end)
                const paramName = func.params[i];
                const cleanParamName = paramName.replace(/\?$/, '');

                // Resolve Default if missing
                if (argRaw === undefined) {
                    if (func.defaults && func.defaults[cleanParamName] !== undefined) {
                        argRaw = func.defaults[cleanParamName];
                    } else if (paramName.endsWith('?')) {
                        // Optional with no default -> pass undefined
                        argRaw = undefined;
                    } else {
                        // Required param missing
                        throw new Error(`Missing required argument '${cleanParamName}'`);
                    }
                }

                // If effective arg is still undefined (optional without default)
                if (argRaw === undefined) {
                    argValues.push(undefined);
                    continue;
                }


                // STRICTNESS CHECK: Parameter Case
                // Handle optional marker (?)
                // cleanParamName already defined above
                const isParamFunction = /^[A-Z]/.test(cleanParamName); // Uppercase = expects Function
                const isParamValue = /^[a-z]/.test(cleanParamName);     // Lowercase = expects Value

                // CHECK FOR EXPLICIT LAMBDA: "var -> expr" or "(var1, var2, ...) -> expr"
                // Single param: x -> x^2
                // Multi param: (x, i) -> i*x^2
                const singleLambdaMatch = argRaw.match(/^([a-zA-Z][a-zA-Z0-9_]*)\s*->\s*(.+)$/);
                const multiLambdaMatch = argRaw.match(/^\(([^)]+)\)\s*->\s*(.+)$/);

                const lambdaMatch = singleLambdaMatch || multiLambdaMatch;

                if (lambdaMatch) {
                    if (!isParamFunction) {
                        // Case: Parameter is lowercase (value), but passed lambda.
                        throw new Error(`Argument mismatch for '${cleanParamName}': Expected value (compatible with lowercase), got Lambda function.`);
                    }

                    // Create Anonymous Function
                    const [, lambdaParamsRaw, lambdaBody] = lambdaMatch;
                    // Parse params - could be single "x" or comma-separated "x, i"
                    const lambdaParams = lambdaParamsRaw.split(',').map(p => p.trim()).filter(p => p);

                    // Freeze the body for static scoping (same as handleFunctionDefinition)
                    const paramSet = new Set(lambdaParams);
                    const staticBody = this.freezeExpression(lambdaBody.trim(), paramSet);

                    // Use namespaced Format: @@Anon@<Timestamp>_<Random>
                    // This satisfies the functionCallRegex logic.
                    const anonName = `@@Anon@${Date.now()}_${Math.floor(Math.random() * 1000)}`;

                    this.functions.set(anonName, {
                        params: lambdaParams,
                        body: staticBody,
                        type: 'def',
                        doc: 'Anonymous Lambda'
                    });
                    argValues.push(anonName);
                    continue;
                }

                // Normal Evaluation
                // Special handling if expecting function: preserve name if simple identifier
                if (isParamFunction) {
                    const trimmed = argRaw.trim();
                    // If it is a simple identifier, check if it is a function
                    // Regex must allow @@Mod@Name format
                    if (/^(?:@@[a-zA-Z0-9_]+@)?(?:@?[a-zA-Z0-9_]+)$/.test(trimmed)) {
                        // Normalize: strip @ but respect @@, then apply case normalization
                        const stripped = trimmed.startsWith("@@") ? trimmed : (trimmed.startsWith("@") ? trimmed.substring(1) : trimmed);
                        const norm = this.normalizeName(stripped);
                        if (this.functions.has(norm)) {
                            argValues.push(norm);
                            continue;
                        } else {
                            // Not found function
                            throw new Error(`Argument mismatch for '${cleanParamName}': Expected existing function, got unknown '${trimmed}'`);
                        }
                    } else {
                        throw new Error(`Argument mismatch for '${cleanParamName}': Expected function name or lambda, got expression.`);
                    }
                }

                // Expecting Value
                // console.log(`Evaluating arg ${cleanParamName}: '${argRaw}'`);
                const result = this.evaluateExpression(argRaw);
                if (result.type === "error") {
                    // console.log("Arg eval error:", result.message);
                    return result;
                }

                // Sanity check: Ensure not a function name string being passed for a value assumption?
                // The prompt says "Variables or expressions that resolve to stuff that is compatible with lower case letter variables".
                // Basically data values.
                argValues.push(result.result);
            }

            if (func.type === 'js') {
                // Execute JS Handler with context
                try {
                    // Pass 'this' as context to allow access to variables (e.g. environment settings)
                    const res = func.handler.call(this, ...argValues);
                    return { type: 'expression', result: res };
                } catch (e) {
                    return { type: 'error', message: `JS Function ${funcName} error: ${e.message}` };
                }
            }

            // Standard Function Evaluation
            // Create temporary variable bindings
            const oldValues = new Map();
            // Also need to bind function aliases if passing functions!
            // If param is F, and we pass "Sin", inside body F(x) calls Sin(x).
            // We need to register F as alias to Sin temporarily.
            // Or use localScope? evaluateExpression supports localScope.
            // But currently localScope is single map (name -> value).
            // Logic in evaluateExpression looks up localScope -> if string -> checks function map. (HOC Logic).

            const callBindScope = new Map();

            for (let i = 0; i < func.params.length; i++) {
                const param = func.params[i];
                const cleanParam = param.replace(/\?$/, '');
                callBindScope.set(cleanParam, argValues[i]);
            }

            // Local Scope handling in evaluateExpression is sufficient for HOC if we pass map
            const result = this.evaluateExpression(func.body, callBindScope);

            // Clean up anonymous functions? 
            // Currently they leak into global map. 
            // Ideally we track them and delete, but GC is hard here without extensive tracking.
            // unique names prevent conflict.

            return result;
        } catch (error) {
            return {
                type: "error",
                message: `Function call error: ${error.message}`,
            };
        }
    }

    /**
     * Handle special functions (SUM, PROD, SEQ)
     */
    handleSpecialFunction(
        keyword,
        variable,
        expression,
        startStr,
        endStr,
        incrementStr,
    ) {
        try {
            // Evaluate bounds and increment
            const startResult = this.evaluateExpression(startStr);
            if (startResult.type === "error") return startResult;

            const endResult = this.evaluateExpression(endStr);
            if (endResult.type === "error") return endResult;

            const incrementResult = this.evaluateExpression(incrementStr);
            if (incrementResult.type === "error") return incrementResult;

            // Convert to integers for iteration
            const start = this.toInteger(startResult.result);
            const end = this.toInteger(endResult.result);
            const increment = this.toInteger(incrementResult.result);

            if (increment <= 0) {
                return {
                    type: "error",
                    message: "Increment must be positive integer",
                };
            }

            if (end < start) {
                return {
                    type: "error",
                    message: "The end cannot be less than start",
                };
            }

            // Save current variable value
            const oldValue = this.variables.get(variable);

            let result;
            let iterationCount = 0;
            let interrupted = false;
            let progressCallback = this.progressCallback;

            // For SEQ, we need to collect all values
            const values = keyword === "SEQ" ? [] : null;

            // Initialize accumulator for SUM/PROD
            let accumulator = null;
            if (keyword === "SUM") {
                accumulator = new Integer(0);
            } else if (keyword === "PROD") {
                accumulator = new Integer(1);
            }

            for (let i = start; i <= end; i += increment) {
                iterationCount++;

                // Check for interruption on every iteration
                if (progressCallback) {
                    const shouldContinue = progressCallback(
                        keyword,
                        variable,
                        i,
                        end,
                        accumulator,
                        iterationCount,
                    );
                    if (!shouldContinue) {
                        interrupted = true;
                        break;
                    }
                }

                this.variables.set(variable, new Integer(i));
                const exprResult = this.evaluateExpression(expression);
                if (exprResult.type === "error") {
                    this.restoreVariable(variable, oldValue);
                    return exprResult;
                }

                // Directly accumulate for SUM/PROD, or collect for SEQ
                if (keyword === "SUM") {
                    accumulator = accumulator.add(exprResult.result);
                } else if (keyword === "PROD") {
                    accumulator = accumulator.multiply(exprResult.result);
                } else if (keyword === "SEQ") {
                    values.push(exprResult.result);
                }
            }

            // Restore variable
            this.restoreVariable(variable, oldValue);

            if (interrupted) {
                return {
                    type: "error",
                    message: `${keyword} computation interrupted at ${variable}=${start + (iterationCount - 1) * increment} (${iterationCount} iterations completed, current value: ${this.formatValue(accumulator || (values && values[values.length - 1]))})`,
                };
            }

            // Set result based on keyword
            if (iterationCount === 0) {
                result = keyword === "PROD" ? new Integer(1) : new Integer(0);
            } else if (keyword === "SUM" || keyword === "PROD") {
                result = accumulator;
            } else if (keyword === "SEQ") {
                result = {
                    type: "sequence",
                    values: values,
                    lastValue: values[values.length - 1],
                };
            }

            return {
                type: "expression",
                result: result,
            };
        } catch (error) {
            return {
                type: "error",
                message: `${keyword} error: ${error.message}`,
            };
        }
    }

    /**
     * Evaluate an expression with variable substitution and function calls
     */
    evaluateExpression(expression, localScope = [new Map()]) {
        if (typeof expression !== 'string') {
            console.error("evaluateExpression called with non-string:", expression);
            // console.trace();
            throw new Error("evaluateExpression requires a string expression");
        }
        // console.log("Evaluating:", expression);
        try {
            // Normalize Scope Chain
            const scopeChain = Array.isArray(localScope) ? localScope : [localScope];

            // Quick check: if expression is a simple variable name containing an oracle, return it directly
            const trimmed = expression.trim();
            if (/^@?[a-z_][a-zA-Z0-9_]*$/i.test(trimmed)) {
                const varName = trimmed.startsWith('@') ? trimmed.substring(1) : trimmed;
                const normalizedName = this.normalizeName(varName);
                // Check scope chain first
                for (const scope of scopeChain) {
                    if (scope.has(normalizedName)) {
                        const val = scope.get(normalizedName);
                        if (typeof val === 'function' && val.yes) {
                            return { type: 'expression', result: val };
                        }
                        break;
                    }
                }
                // Check global variables
                if (this.variables.has(normalizedName)) {
                    const val = this.variables.get(normalizedName);
                    if (typeof val === 'function' && val.yes) {
                        return { type: 'expression', result: val };
                    }
                }
            }

            // Oracle arithmetic handling: detect expressions with oracle variables and arithmetic operators
            // This handles cases like c+c, 2*c, c*2, c+1, etc.
            const oracleArithResult = this.tryOracleArithmetic(trimmed, scopeChain);
            if (oracleArithResult !== null) {
                return { type: 'expression', result: oracleArithResult };
            }

            // Check for Lambda Expression (x -> x^2)
            // If expression is a lambda, we register it and return the name
            const lambdaMatch = expression.match(/^\s*([a-zA-Z][a-zA-Z0-9_]*)\s*->\s*(.+)$/);
            if (lambdaMatch) {
                const [, lParam, lBody] = lambdaMatch;
                const anonName = `@@Anon@Lambda_${Date.now()}_${Math.floor(Math.random() * 1000)}`;
                this.functions.set(anonName, {
                    params: [lParam.trim()],
                    body: lBody.trim(),
                    type: 'def',
                    doc: 'Anonymous Lambda Expression'
                });
                return { type: 'expression', result: anonName };
            }

            // Check for Piecewise syntax {{cond ? val, cond2 ? val2, default}}
            // Must check BEFORE object literal since both use braces
            const trimmedExpr = expression.trim();
            if ((trimmedExpr.startsWith('{{') && trimmedExpr.endsWith('}}')) ||
                (trimmedExpr.startsWith('{') && trimmedExpr.endsWith('}') && trimmedExpr.includes('?'))) {
                // Determine if this is piecewise (has ?) or object literal (has =)
                const inner = trimmedExpr.startsWith('{{')
                    ? trimmedExpr.slice(2, -2).trim()
                    : trimmedExpr.slice(1, -1).trim();

                // Check if it looks like piecewise (contains ? but first non-nested special char is ?)
                // vs object literal (contains = as assignment)
                let isPiecewise = false;
                let depth = 0;
                let inString = false;
                for (let i = 0; i < inner.length; i++) {
                    const char = inner[i];
                    if (char === '"') {
                        let backslashCount = 0;
                        let j = i - 1;
                        while (j >= 0 && inner[j] === '\\') { backslashCount++; j--; }
                        if (backslashCount % 2 === 0) inString = !inString;
                    }
                    if (!inString) {
                        if (char === '(' || char === '[' || char === '{') depth++;
                        else if (char === ')' || char === ']' || char === '}') depth--;
                        else if (depth === 0) {
                            if (char === '?') {
                                isPiecewise = true;
                                break;
                            } else if (char === '=') {
                                // Check if this = is part of ==, >=, <=, or != (comparison operators)
                                const prevChar = i > 0 ? inner[i - 1] : '';
                                const nextChar = i + 1 < inner.length ? inner[i + 1] : '';
                                if (prevChar === '>' || prevChar === '<' || prevChar === '!' || prevChar === '=' || nextChar === '=') {
                                    // This is part of a comparison operator, continue
                                    continue;
                                }
                                // Found = that's not ==, >=, <=, !=, so it's object literal
                                break;
                            }
                        }
                    }
                }

                if (isPiecewise) {
                    // Parse piecewise: condition ? value pairs, last can be just value (default)
                    const pieces = [];
                    let defaultValue = null;

                    // Split by comma at depth 0
                    const parts = [];
                    let current = '';
                    depth = 0;
                    inString = false;
                    for (let i = 0; i < inner.length; i++) {
                        const char = inner[i];
                        if (char === '"') {
                            let backslashCount = 0;
                            let j = i - 1;
                            while (j >= 0 && inner[j] === '\\') { backslashCount++; j--; }
                            if (backslashCount % 2 === 0) inString = !inString;
                        }
                        if (!inString) {
                            if (char === '(' || char === '[' || char === '{') depth++;
                            else if (char === ')' || char === ']' || char === '}') depth--;
                        }
                        if (char === ',' && depth === 0 && !inString) {
                            parts.push(current.trim());
                            current = '';
                        } else {
                            current += char;
                        }
                    }
                    if (current.trim()) parts.push(current.trim());

                    // Parse each part as "condition ? value" or just "value" (default)
                    for (let i = 0; i < parts.length; i++) {
                        const part = parts[i];

                        // Find ? at depth 0
                        let qIndex = -1;
                        depth = 0;
                        inString = false;
                        for (let j = 0; j < part.length; j++) {
                            const char = part[j];
                            if (char === '"') {
                                let backslashCount = 0;
                                let k = j - 1;
                                while (k >= 0 && part[k] === '\\') { backslashCount++; k--; }
                                if (backslashCount % 2 === 0) inString = !inString;
                            }
                            if (!inString) {
                                if (char === '(' || char === '[' || char === '{') depth++;
                                else if (char === ')' || char === ']' || char === '}') depth--;
                                else if (char === '?' && depth === 0) {
                                    qIndex = j;
                                    break;
                                }
                            }
                        }

                        if (qIndex === -1) {
                            // No ?, this is the default value (must be last)
                            if (i !== parts.length - 1) {
                                throw new Error(`Piecewise: default value must be last, but found unconditional at position ${i + 1}`);
                            }
                            const valResult = this.evaluateExpression(part, scopeChain);
                            if (valResult.type === 'error') throw new Error(valResult.message);
                            defaultValue = valResult.result;
                        } else {
                            // condition ? value
                            const condExpr = part.slice(0, qIndex).trim();
                            const valExpr = part.slice(qIndex + 1).trim();

                            const condResult = this.evaluateExpression(condExpr, scopeChain);
                            if (condResult.type === 'error') throw new Error(`Piecewise condition error: ${condResult.message}`);

                            // Check if condition is truthy
                            const cond = condResult.result;
                            let isTruthy = false;
                            if (cond instanceof Integer) {
                                isTruthy = cond.value !== 0n;
                            } else if (cond instanceof Rational) {
                                isTruthy = cond.sign() !== 0;
                            } else if (typeof cond === 'number') {
                                isTruthy = cond !== 0;
                            } else if (typeof cond === 'bigint') {
                                isTruthy = cond !== 0n;
                            }

                            if (isTruthy) {
                                // Return this value immediately
                                const valResult = this.evaluateExpression(valExpr, scopeChain);
                                if (valResult.type === 'error') throw new Error(valResult.message);
                                return { type: 'expression', result: valResult.result };
                            }
                            // Otherwise continue to next condition
                        }
                    }

                    // No condition matched
                    if (defaultValue !== null) {
                        return { type: 'expression', result: defaultValue };
                    }
                    throw new Error("Piecewise: no matching condition and no default value");
                }
            }

            // Check for Object Literal {a=5, b=c, Der=x->x^2}
            if (trimmedExpr.startsWith('{') && trimmedExpr.endsWith('}') && !trimmedExpr.startsWith('{{')) {
                const inner = trimmedExpr.slice(1, -1).trim();
                if (inner.length === 0) {
                    // Empty object
                    return { type: 'expression', result: { type: 'object', properties: new Map() } };
                }

                // Parse comma-separated key=value pairs, respecting nested brackets/parens
                const pairs = [];
                let current = '';
                let depth = 0;
                let inString = false;

                for (let i = 0; i < inner.length; i++) {
                    const char = inner[i];
                    if (char === '"') {
                        let backslashCount = 0;
                        let j = i - 1;
                        while (j >= 0 && inner[j] === '\\') { backslashCount++; j--; }
                        if (backslashCount % 2 === 0) inString = !inString;
                    }
                    if (!inString) {
                        if (char === '(' || char === '[' || char === '{') depth++;
                        else if (char === ')' || char === ']' || char === '}') depth--;
                    }
                    if (char === ',' && depth === 0 && !inString) {
                        pairs.push(current.trim());
                        current = '';
                    } else {
                        current += char;
                    }
                }
                if (current.trim()) pairs.push(current.trim());

                // Parse each pair as key=value
                const properties = new Map();
                for (const pair of pairs) {
                    // Find first = that's not inside nested structure
                    let eqIndex = -1;
                    let pDepth = 0;
                    let pInString = false;
                    for (let i = 0; i < pair.length; i++) {
                        const char = pair[i];
                        if (char === '"') {
                            let backslashCount = 0;
                            let j = i - 1;
                            while (j >= 0 && pair[j] === '\\') { backslashCount++; j--; }
                            if (backslashCount % 2 === 0) pInString = !pInString;
                        }
                        if (!pInString) {
                            if (char === '(' || char === '[' || char === '{') pDepth++;
                            else if (char === ')' || char === ']' || char === '}') pDepth--;
                            else if (char === '=' && pDepth === 0) {
                                eqIndex = i;
                                break;
                            }
                        }
                    }

                    if (eqIndex === -1) {
                        throw new Error(`Invalid object literal: missing '=' in '${pair}'`);
                    }

                    const key = pair.slice(0, eqIndex).trim();
                    const valueExpr = pair.slice(eqIndex + 1).trim();

                    // Evaluate the value expression
                    const valueResult = this.evaluateExpression(valueExpr, scopeChain);
                    if (valueResult.type === 'error') {
                        throw new Error(`Error evaluating '${key}': ${valueResult.message}`);
                    }
                    properties.set(key, valueResult.result);
                }

                return { type: 'expression', result: { type: 'object', properties } };
            }

            // Helper to lookup variable in chain
            // Note: lookup logic is embedded in Variable Substitution section below or logic uses map merge for regex
            // But for explicit checks we need helpers
            // Apply name normalization for case-insensitive lookup
            const hasVar = (name) => {
                const normalized = this.normalizeName(name);
                for (const scope of scopeChain) {
                    if (scope.has(normalized)) return true;
                    // Fallback for @ prefix
                    if (name.startsWith('@') && scope.has(this.normalizeName(name.substring(1)))) return true;
                }
                if (this.variables.has(normalized)) return true;
                if (name.startsWith('@') && this.variables.has(this.normalizeName(name.substring(1)))) return true;
                return false;
            };
            const getVar = (name) => {
                const normalized = this.normalizeName(name);
                for (const scope of scopeChain) {
                    if (scope.has(normalized)) return scope.get(normalized);
                    // Fallback for @ prefix
                    const strippedNorm = this.normalizeName(name.substring(1));
                    if (name.startsWith('@') && scope.has(strippedNorm)) return scope.get(strippedNorm);
                }
                if (this.variables.has(normalized)) return this.variables.get(normalized);
                if (name.startsWith('@') && this.variables.has(this.normalizeName(name.substring(1)))) {
                    return this.variables.get(this.normalizeName(name.substring(1)));
                }
                return undefined;
            };

            // Check for temp base commands
            const baseCommandMatch = expression.match(/^([A-Z0-9]+)\s+(.+)$/);
            if (baseCommandMatch) {
                const command = baseCommandMatch[1];
                const rest = baseCommandMatch[2];
                let tempBase = null;

                const upperCommand = command.toUpperCase();
                if (upperCommand === "HEX" || upperCommand === "0X") tempBase = BaseSystem.HEXADECIMAL;
                else if (upperCommand === "BIN" || upperCommand === "0B") tempBase = BaseSystem.BINARY;
                else if (upperCommand === "OCT" || upperCommand === "0O") tempBase = BaseSystem.OCTAL;
                else if (upperCommand === "DEC" || command === "0z[10]") tempBase = BaseSystem.DECIMAL;
                else if (command.startsWith("BASE")) {
                    const match = command.match(/^BASE(\d+)$/);
                    if (match) {
                        const baseNum = parseInt(match[1]);
                        if (baseNum >= 2 && baseNum <= 62) {
                            try { tempBase = BaseSystem.fromBase(baseNum); }
                            catch (e) { }
                        }
                    }
                }

                if (tempBase) {
                    const originalBase = this.inputBase;
                    try {
                        this.setInputBase(tempBase);
                        return this.evaluateExpression(rest, scopeChain);
                    } finally {
                        this.setInputBase(originalBase);
                    }
                }
            }

            let substitutedFunctions = expression;
            const DEBUG_PROP_CALL = false; // Set to true to debug property calls

            // Property-based function call substitution (P.Der(5) -> resolvedFunc(5), a.list(1) -> listFunc(1))
            // Must happen before regular function call handling
            const propertyCallRegex = /([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)\s*\(/g;
            let propMatch;
            while ((propMatch = propertyCallRegex.exec(substitutedFunctions)) !== null) {
                const targetName = propMatch[1];
                const propName = propMatch[2];
                const normalizedTarget = this.normalizeName(targetName);
                // Don't normalize property name - properties are stored with original case

                // Check if target has this property
                if (this.hasDecoration(normalizedTarget, propName)) {
                    const propValue = this.getDecoration(normalizedTarget, propName);
                    let funcName = null;

                    // If property is a string (function name), use it
                    if (propValue && propValue.type === 'string' && propValue.value) {
                        funcName = propValue.value;
                    }
                    // If property is an internal function reference (for lists/objects)
                    else if (propValue && typeof propValue === 'string' && this.functions.has(propValue)) {
                        funcName = propValue;
                    }
                    // If property is a sequence (list), create a temporary list accessor
                    else if (propValue && propValue.type === 'sequence') {
                        const tempListName = `@@TempList@${Date.now()}_${Math.random().toString(36).slice(2)}`;
                        // Store with normalized name so function lookup can find it
                        const normalizedTempName = this.normalizeName(tempListName);
                        this.functions.set(normalizedTempName, {
                            type: 'list_accessor',
                            list: propValue  // Pass the full sequence object, not just values
                        });
                        funcName = normalizedTempName;
                    }

                    if (funcName) {
                        // Replace "Target.Prop(" with "funcName("
                        const fullPropertyCall = `${targetName}.${propName}`;
                        const before = substitutedFunctions;
                        substitutedFunctions = substitutedFunctions.replace(
                            new RegExp(fullPropertyCall.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '\\s*\\(', 'g'),
                            funcName + '('
                        );
                        if (DEBUG_PROP_CALL) console.log('[PROP_CALL] Substituted:', before, '->', substitutedFunctions);
                        propertyCallRegex.lastIndex = 0; // Reset regex after substitution
                    }
                }
            }
            if (DEBUG_PROP_CALL && substitutedFunctions !== expression) console.log('[PROP_CALL] Final:', substitutedFunctions);

            // Function Call Substitution
            // Matches: Name(args) including @@Anon@123_456 anonymous function names
            const functionCallRegex = /(?:^|[^a-zA-Z0-9_@])((?:@@[a-zA-Z0-9_]+@[a-zA-Z0-9_]+)|(?:@?[_a-zA-Z][a-zA-Z0-9_]*))\s*\(/g;
            let match;
            while ((match = functionCallRegex.exec(substitutedFunctions)) !== null) {
                const fullMatch = match[0];
                const funcName = match[1];
                const prefixLen = fullMatch.indexOf(funcName);
                const startIndex = match.index + prefixLen;
                const openParenIndex = startIndex + funcName.length;

                // Find matching closing parenthesis
                let depth = 1;
                let closeParenIndex = -1;

                // Context-aware paren matching (skip strings)
                let inCallStr = false;

                for (let i = openParenIndex + 1; i < substitutedFunctions.length; i++) {
                    const c = substitutedFunctions[i];
                    if (c === '"') {
                        let backslashCount = 0;
                        let j = i - 1;
                        while (j >= 0 && substitutedFunctions[j] === '\\') { backslashCount++; j--; }
                        if (backslashCount % 2 === 0) inCallStr = !inCallStr;
                    }

                    if (!inCallStr) {
                        if (c === '(') depth++;
                        else if (c === ')') depth--;
                    }

                    if (depth === 0) {
                        closeParenIndex = i;
                        break;
                    }
                }

                if (closeParenIndex !== -1) {
                    const argsStr = substitutedFunctions.substring(openParenIndex + 1, closeParenIndex);
                    // Normalize function name with case normalization
                    const normalizedFuncName = this.normalizeName(funcName.startsWith("@@") ? funcName : (funcName.startsWith("@") ? funcName.substring(1) : funcName));
                    let funcDef = this.functions.get(normalizedFuncName);

                    // Alias Lookup in Scope Chain
                    if (!funcDef) {
                        const aliasVal = getVar(normalizedFuncName);
                        if (typeof aliasVal === 'string') {
                            const aliasNorm = this.normalizeName(aliasVal.startsWith("@@") ? aliasVal : (aliasVal.startsWith("@") ? aliasVal.substring(1) : aliasVal));
                            if (this.functions.has(aliasNorm)) {
                                funcDef = this.functions.get(aliasNorm);
                            } else {
                                // Check if it is a lambda string (e.g. "x -> x^2")
                                const lambdaMatch = aliasVal.match(/^\s*([a-zA-Z][a-zA-Z0-9_]*)\s*->\s*(.+)$/);
                                if (lambdaMatch) {
                                    const [, lParam, lBody] = lambdaMatch;
                                    funcDef = {
                                        params: [lParam.trim()],
                                        body: lBody.trim(),
                                        type: 'def',
                                        doc: 'Dynamic Lambda'
                                    };
                                }
                            }
                        }
                    }

                    if (funcDef) {
                        // Parse arguments respecting parens/brackets/quotes
                        const args = [];
                        let currentArg = "";
                        let argDepth = 0;
                        let inArgStr = false;

                        for (let i = 0; i < argsStr.length; i++) {
                            const char = argsStr[i];
                            if (char === '"') {
                                let backslashCount = 0;
                                let j = i - 1;
                                while (j >= 0 && argsStr[j] === '\\') { backslashCount++; j--; }
                                if (backslashCount % 2 === 0) inArgStr = !inArgStr;
                            }

                            if (!inArgStr) {
                                if (char === '(' || char === '[' || char === '{') argDepth++;
                                else if (char === ')' || char === ']' || char === '}') argDepth--;
                            }

                            if (char === ',' && argDepth === 0 && !inArgStr) {
                                args.push(currentArg.trim());
                                currentArg = "";
                            } else {
                                currentArg += char;
                            }
                        }
                        if (currentArg.trim() !== "") args.push(currentArg.trim());
                        else if (argsStr.trim() === "") { /* empty args */ }

                        // Handle List Accessor
                        if (funcDef.type === 'list_accessor') {
                            if (args.length !== 1) throw new Error(`List accessor '${funcName}' expects 1 argument (index)`);
                            const indexRes = this.evaluateExpression(args[0], scopeChain);
                            if (indexRes.type === 'error') throw new Error(indexRes.message);

                            const indexVal = this.toInteger(indexRes.result);
                            const list = funcDef.list; // Sequence object
                            const valArr = list.values;

                            let actualIndex = indexVal;
                            if (indexVal === 0) {
                                // Return full list
                                const resultStr = this.formatValueWithPrefix(list);
                                substitutedFunctions = substitutedFunctions.substring(0, startIndex) + resultStr + substitutedFunctions.substring(closeParenIndex + 1);
                                functionCallRegex.lastIndex = 0;
                                continue;
                            }

                            // 1-based indexing for elements
                            if (actualIndex > 0) actualIndex = actualIndex - 1;
                            if (actualIndex < 0) actualIndex = valArr.length + actualIndex; // handle negative

                            if (actualIndex < 0 || actualIndex >= valArr.length) {
                                throw new Error(`Index ${indexVal} out of bounds for list of length ${valArr.length}`);
                            }

                            const val = valArr[actualIndex];
                            const resultStr = this.formatValueWithPrefix(val);
                            substitutedFunctions = substitutedFunctions.substring(0, startIndex) +
                                resultStr +
                                substitutedFunctions.substring(closeParenIndex + 1);

                            functionCallRegex.lastIndex = 0;
                            continue;
                        }

                        // Function Logic
                        const callBindScope = new Map();
                        const argValues = [];

                        if (funcDef.lazy) {
                            // Lazy Evaluation: Pass raw strings
                            for (const arg of args) argValues.push(arg);
                            // Need to expose scopeChain to handler?
                            // We don't have a mechanism to pass scopeChain to JS function explicitly in 'args'.
                            // We attach it to instance state temporarily.
                        } else {
                            // Eager Evaluation
                            for (let argIdx = 0; argIdx < args.length; argIdx++) {
                                const arg = args[argIdx];
                                const paramName = funcDef.params[argIdx] || '';
                                const cleanParamName = paramName.replace(/\?$/, '');
                                // Check if parameter expects a function (uppercase start)
                                const isParamFunction = /^[A-Z]/.test(cleanParamName);

                                // Support both single-param (x -> expr) and multi-param ((x, i) -> expr) lambdas
                                const singleLambdaMatch = arg.match(/^([a-zA-Z][a-zA-Z0-9_]*)\s*->\s*(.+)$/);
                                const multiLambdaMatch = arg.match(/^\(([^)]+)\)\s*->\s*(.+)$/);
                                const lambdaMatch = singleLambdaMatch || multiLambdaMatch;

                                if (lambdaMatch) {
                                    const [, lParamsRaw, lBody] = lambdaMatch;
                                    const lParams = lParamsRaw.split(',').map(p => p.trim()).filter(p => p);

                                    // Freeze the body for static scoping
                                    const paramSet = new Set(lParams);
                                    const staticBody = this.freezeExpression(lBody.trim(), paramSet);

                                    const anonName = `@@Anon@Lambda_${Date.now()}_${Math.floor(Math.random() * 1000)}`;
                                    this.functions.set(anonName, {
                                        params: lParams,
                                        body: staticBody,
                                        type: 'def',
                                        doc: 'Anonymous Lambda'
                                    });
                                    argValues.push(anonName);
                                } else if (arg.trim() === '') {
                                    argValues.push(undefined);
                                } else {
                                    // Check if arg is a simple identifier that could be a function name
                                    const trimmed = arg.trim();
                                    if (/^(?:@@[a-zA-Z0-9_]+@)?(?:@?[A-Z][a-zA-Z0-9_]*)$/.test(trimmed)) {
                                        // Looks like a function name (uppercase start)
                                        const stripped = trimmed.startsWith("@@") ? trimmed : (trimmed.startsWith("@") ? trimmed.substring(1) : trimmed);
                                        const norm = this.normalizeName(stripped);
                                        if (this.functions.has(norm)) {
                                            // Pass function name as string for HOC support
                                            if (isParamFunction) {
                                                argValues.push(norm);
                                            } else {
                                                // Lowercase param - pass the normalized name as string
                                                argValues.push(norm);
                                            }
                                            continue;
                                        }
                                    }
                                    // Evaluate as expression
                                    const r = this.evaluateExpression(arg, scopeChain);
                                    if (r.type === 'error') throw new Error(r.message);
                                    argValues.push(r.result);
                                }
                            }
                        }

                        if (!funcDef.lazy) {
                            // Bind Params
                            // Use outer callBindScope
                            // const callBindScope = new Map(); // Don't redeclare!
                            for (let i = 0; i < funcDef.params.length; i++) {
                                const p = funcDef.params[i];
                                const cleanP = p.replace(/\?$/, '');
                                if (i < argValues.length && argValues[i] !== undefined) {
                                    callBindScope.set(cleanP, argValues[i]);
                                }
                                else if (funcDef.defaults && funcDef.defaults[cleanP] !== undefined) {
                                    // Evaluate default value (static scope)
                                    const dRes = this.evaluateExpression(funcDef.defaults[cleanP], []);
                                    if (dRes.type !== 'error') callBindScope.set(cleanP, dRes.result);
                                } else if (p.endsWith("?")) {
                                    callBindScope.set(cleanP, undefined);
                                } else {
                                    throw new Error(`Missing required argument '${cleanP}'`);
                                }
                            }

                            // Inject @@ (All Arguments as List)
                            const seq = { type: 'sequence', values: argValues };
                            callBindScope.set("@@", seq);
                        }

                        // Execution
                        let resultVal;
                        if (funcDef.type === 'js' || typeof funcDef.handler === 'function') {
                            this._currentScopeChain = scopeChain; // Allow JS func to access scope
                            this._currentCallScope = callBindScope;
                            try {
                                resultVal = funcDef.handler.call(this, ...argValues);
                            } finally {
                                this._currentScopeChain = null;
                                this._currentCallScope = null;
                            }
                        } else {
                            // User Defined Function (Def)
                            // Scope: [callBindScope, ...scopeChain] (Dynamic Scoping per user request)
                            // "assignment via ASSIGN should be local... GLOBAL global"
                            const newChain = [callBindScope, ...scopeChain];
                            const r = this.evaluateExpression(funcDef.body, newChain);
                            if (r.type === 'error') throw new Error(r.message);
                            resultVal = r.result;
                        }

                        // If the function call is the entire expression and returns a special type,
                        // return it directly to avoid re-parsing issues
                        if (startIndex === 0 && closeParenIndex === substitutedFunctions.length - 1) {
                            if (resultVal && resultVal.type === 'string') {
                                return { type: "expression", result: resultVal };
                            }
                            // Return oracles directly (function with yes property)
                            if (typeof resultVal === 'function' && resultVal.yes) {
                                return { type: "expression", result: resultVal };
                            }
                            // Return Promises directly for async functions
                            if (resultVal && typeof resultVal.then === 'function') {
                                return { type: "expression", result: resultVal };
                            }
                        }

                        const resultStr = this.formatValueWithPrefix(resultVal);

                        substitutedFunctions = substitutedFunctions.substring(0, startIndex) +
                            resultStr +
                            substitutedFunctions.substring(closeParenIndex + 1);

                        functionCallRegex.lastIndex = 0;
                        continue;
                    }
                }
            }

            // Early check: if the entire expression after function substitution is just a string literal,
            // return it directly without further parsing (avoids issues with special chars in strings)
            const trimmedSub = substitutedFunctions.trim();
            const stringOnlyMatch = trimmedSub.match(/^"((?:[^"\\]|\\.)*)"\s*$/);
            if (stringOnlyMatch) {
                const unescaped = stringOnlyMatch[1]
                    .replace(/\\n/g, '\n')
                    .replace(/\\r/g, '\r')
                    .replace(/\\"/g, '"')
                    .replace(/\\\\/g, '\\');
                return { type: "expression", result: { type: "string", value: unescaped } };
            }

            // Variable Substitution
            // We substitute variables with their values.
            // MUST respect string literals!

            let finalExpr = "";
            let i = 0;
            let inString = false;
            let chunkStart = 0; // Optimization: accumulate chunk

            // Build regex for simple next-token identification
            // This is "manual" scanner loop

            while (i < substitutedFunctions.length) {
                const char = substitutedFunctions[i];
                if (char === '"') {
                    // string handling
                    if (!inString) {
                        inString = true;
                    } else {
                        // escape check
                        let backslashCount = 0;
                        let j = i - 1;
                        while (j >= 0 && substitutedFunctions[j] === '\\') { backslashCount++; j--; }
                        if (backslashCount % 2 === 0) inString = false;
                    }
                    finalExpr += char;
                    i++;
                    continue;
                }

                if (inString) {
                    finalExpr += char;
                    i++;
                    continue;
                }

                // Check for variable start
                // If char is start of identifier...
                // [a-zA-Z_@]
                if (/[a-zA-Z_@]/.test(char)) {
                    // Check if Word Start (prev char was not word char)
                    const prev = i > 0 ? substitutedFunctions[i - 1] : " ";
                    // Word chars: [a-zA-Z0-9_@] for our identifiers?
                    // Regex for identifiers: (?:@@[a-zA-Z0-9_]+@)?(?:@?[_a-zA-Z][a-zA-Z0-9_]*)
                    // If prev is part of identifier class, we are inside word.
                    if (/[a-zA-Z0-9_@]/.test(prev)) {
                        finalExpr += char;
                        i++;
                        continue;
                    }

                    // Match identifier from here (including potential dot notation for property access)
                    const tail = substitutedFunctions.substring(i);
                    // Match identifier with optional .property suffix
                    // Match identifier with optional chained .property suffixes (a.obj.m)
                    const match = tail.match(/^((?:@@[a-zA-Z0-9_]+@)?(?:@?[_a-zA-Z][a-zA-Z0-9_]*))(\.[_a-zA-Z][a-zA-Z0-9_]*)*/);

                    if (match) {
                        const fullMatch = match[0];
                        const token = match[1];

                        // Check for chained property access (a.obj.m)
                        if (fullMatch.includes('.')) {
                            const parts = fullMatch.split('.');
                            const baseName = parts[0];
                            const normalizedBase = this.normalizeName(baseName.startsWith("@") ? baseName.substring(1) : baseName);
                            const baseExists = hasVar(baseName) || this.functions.has(normalizedBase);

                            if (baseExists && parts.length > 1) {
                                // Resolve property chain
                                let currentTarget = normalizedBase;
                                let currentValue = null; // Track current object value for nested access
                                let resolved = true;
                                let finalValue = null;

                                // Check if base is an object variable with internal properties
                                const baseVar = this.variables.get(normalizedBase);
                                if (baseVar && baseVar.type === 'object' && baseVar.properties) {
                                    currentValue = baseVar;
                                }

                                for (let pi = 1; pi < parts.length; pi++) {
                                    const propName = parts[pi];
                                    let foundInObject = false;

                                    // If we have a current object value with properties Map, access it directly
                                    if (currentValue && currentValue.type === 'object' && currentValue.properties) {
                                        if (currentValue.properties.has(propName)) {
                                            finalValue = currentValue.properties.get(propName);
                                            currentValue = finalValue; // For further chaining if it's also an object
                                            foundInObject = true;
                                            if (finalValue && finalValue.type === 'object') {
                                                continue;
                                            }
                                            // If this is the last property or value is not an object, we're done
                                            if (pi === parts.length - 1) {
                                                continue;
                                            }
                                            // Not an object but more properties to resolve - check decorations
                                            currentValue = null;
                                        }
                                    }

                                    // If not found in object properties, look up in decorations
                                    if (!foundInObject) {
                                        if (this.hasDecoration(currentTarget, propName)) {
                                            const propValue = this.getDecoration(currentTarget, propName);
                                            finalValue = propValue;

                                            // If this property is an object with properties, we can chain further
                                            if (propValue && propValue.type === 'object' && propValue.properties) {
                                                currentValue = propValue;
                                                continue;
                                            }
                                            // If this property is a string function reference to an object
                                            if (propValue && propValue.type === 'string' && this.functions.has(propValue.value)) {
                                                const funcDef = this.functions.get(propValue.value);
                                                if (funcDef && funcDef.type === 'object_function') {
                                                    currentTarget = propValue.value;
                                                    currentValue = null;
                                                    continue;
                                                }
                                            }
                                            // If not the last property, we need to continue chaining
                                            if (pi < parts.length - 1) {
                                                resolved = false;
                                                break;
                                            }
                                        } else {
                                            resolved = false;
                                            break;
                                        }
                                    }
                                }

                                if (resolved && finalValue !== null) {
                                    const s = this.formatValueWithPrefix(finalValue);
                                    finalExpr += s;
                                    i += fullMatch.length;
                                    continue;
                                }
                            }
                        }

                        // Fall back to single property access for backwards compat
                        const singleMatch = tail.match(/^((?:@@[a-zA-Z0-9_]+@)?(?:@?[_a-zA-Z][a-zA-Z0-9_]*))(?:\.([_a-zA-Z][a-zA-Z0-9_]*))?/);
                        if (singleMatch) {
                            const singleToken = singleMatch[1];
                            const propertyName = singleMatch[2];

                            if (propertyName) {
                                const normalizedTarget = this.normalizeName(singleToken.startsWith("@") ? singleToken.substring(1) : singleToken);
                                const targetExists = hasVar(singleToken) || this.functions.has(normalizedTarget);

                                if (targetExists && this.hasDecoration(normalizedTarget, propertyName)) {
                                    const propValue = this.getDecoration(normalizedTarget, propertyName);
                                    const s = this.formatValueWithPrefix(propValue);
                                    finalExpr += s;
                                    i += singleMatch[0].length;
                                    continue;
                                }
                            }
                        }

                        // Check if known variable
                        if (hasVar(token)) {
                            // AMBIGUITY CHECK
                            if (this.inputBase && this.inputBase.isValidString(token)) {
                                // It is both a variable and a valid number in current base
                                // e.g. "a" in HEX.
                                // Variable takes precedence? Or Error?
                                // Test expects Error.
                                // But only if not prefixed with @?
                                if (!token.includes('@')) {
                                    // Check if it really parses as number (isValidString is loose?)
                                    // isValidString checks chars.
                                    return {
                                        type: "error",
                                        message: `Ambiguous reference '${token}'. It is both a variable and a valid number in ${this.inputBase.name}. Use @${token} for variable or 0z[10]${token} (or 0${BaseSystem.getPrefixForSystem(this.inputBase) || 'd'}${token}) for number.`
                                    };
                                }
                            }

                            const val = getVar(token);
                            // If this is an oracle and it's the entire expression, return it directly
                            if (typeof val === 'function' && val.yes && finalExpr === '' && i + token.length === substitutedFunctions.length) {
                                return { type: "expression", result: val };
                            }
                            const s = this.formatValueWithPrefix(val);
                            finalExpr += s;
                            i += token.length;
                            continue;
                        }
                        // Check if known function (used as value) - with normalization
                        else if (this.functions.has(this.normalizeName(token))) { // && !token.includes('@') check disabled strictly
                            // Strict usage: if simple name 'A' and base has 'A', ambiguous?
                            // Logic copied from previous implementation:
                            let isAmbiguous = false;
                            if (this.inputBase) {
                                // check existing logic...
                                // Simplified: if token is digits in base, ERROR unless prefixed with @.
                                if (!token.includes('@')) {
                                    // Check digits
                                    // ...
                                }
                            }
                            // Assuming safe for now or HOC
                            finalExpr += token;
                            i += token.length;
                            continue;
                        }
                    }
                }

                finalExpr += char;
                i++;
            }

            const preprocessed = this.preprocessExpression(finalExpr);
            const normalizedBases = this._normalizePrefixedBaseLiterals(preprocessed);

            const baseOpValue = this._evaluateBaseOperators(normalizedBases, scopeChain);
            if (baseOpValue !== null) {
                return { type: "expression", result: baseOpValue };
            }

            const specialMatch = normalizedBases.match(
                /^(SUM|PROD|SEQ)\[([a-zA-Z])\]\(([^,]+),\s*([^,]+),\s*([^,]+)(?:,\s*([^)]+))?\)$/,
            );
            if (specialMatch) {
                const [, keyword, variable, expr, start, end, increment] = specialMatch;
                return this.handleSpecialFunction(keyword, variable, expr, start, end, increment || "1");
            }

            // Check if the entire expression is just a string literal
            const stringLiteralMatch = normalizedBases.match(/^"((?:[^"\\]|\\.)*)"\s*$/);
            if (stringLiteralMatch) {
                // Unescape the string and return it directly
                const unescaped = stringLiteralMatch[1]
                    .replace(/\\n/g, '\n')
                    .replace(/\\r/g, '\r')
                    .replace(/\\"/g, '"')
                    .replace(/\\\\/g, '\\');
                return { type: "expression", result: { type: "string", value: unescaped } };
            }

            let result;
            try {
                result = Parser.parse(normalizedBases, {
                    typeAware: true,
                    customBases: this.customBases,
                    inputBase: this.inputBase
                });
            } catch (parseError) {
                // Silenced debug log unless critical
                // console.error("evaluateExpression Parse Error on:", preprocessed);
                // console.error("Original error:", parseError.message);

                // HOC fallback
                const trimmed = normalizedBases.trim();
                const tokens = trimmed.split(/[^a-zA-Z0-9@_]/).filter(t => t.length > 0);
                for (const token of tokens) {
                    const rawName = token.startsWith("@@") ? token : (token.startsWith("@") ? token.substring(1) : token);
                    const normalizedName = this.normalizeName(rawName);
                    if (this.functions.has(normalizedName)) {
                        if (tokens.length > 1 || trimmed.includes("(") || trimmed.includes(")")) {
                            throw new Error(`Function '${normalizedName}' cannot be used as a value in this context`);
                        }
                        // Format function reference nicely
                        const funcDef = this.functions.get(normalizedName);
                        if (funcDef.params && funcDef.body) {
                            const params = funcDef.params.join(", ");
                            return { type: "expression", result: `${params} -> ${funcDef.body}` };
                        }
                        return { type: "expression", result: normalizedName };
                    }
                }
                throw parseError;
            }

            return { type: "expression", result: result };
        } catch (error) {
            return { type: "error", message: error.message };
        }
    }

    /**
     * Format a value with 0d prefix for safe substitution in non-decimal bases
     */
    formatValueWithPrefix(value) {
        if (!value) return "0";

        // Handle string type - return as quoted string literal
        if (value.type === "string") {
            // Check if this string is a function reference
            const funcName = value.value;
            if (funcName && this.functions.has(funcName)) {
                const funcDef = this.functions.get(funcName);
                // Return the function name for substitution (it will be called later)
                return funcName;
            }
            // Escape newlines and backslashes for safe substitution
            // Use String.raw or explicit escaping to get literal backslash-n
            const escaped = value.value
                .replace(/\\/g, '\\\\')
                .replace(/\n/g, '\\' + 'n')
                .replace(/\r/g, '\\' + 'r')
                .replace(/"/g, '\\"');
            return `"${escaped}"`;
        }

        if (value.type === "sequence") {
            const formatted = value.values.map(v => this.formatValueWithPrefix(v));
            return `[${formatted.join(", ")}]`;
        }

        if (value.type === "object") {
            const pairs = [];
            for (const [key, val] of value.properties) {
                pairs.push(`${key}=${this.formatValueWithPrefix(val)}`);
            }
            return `{${pairs.join(", ")}}`;
        }

        // Handle Oracle (function with yes property)
        if (typeof value === 'function' && value.yes) {
            return `[Oracle]`;
        }

        // Handle Promise (async results)
        if (value && typeof value.then === 'function') {
            return `[Promise]`;
        }

        if (value instanceof RationalInterval) {
            return `${this.formatValueWithPrefix(value.low)}:${this.formatValueWithPrefix(value.high)}`;
        }

        // Handle numbers
        let str;
        if (value instanceof Rational) {
            str = value.toString();
        } else if (value instanceof Integer) {
            str = value.value.toString();
        } else {
            str = value.toString();
        }

        // Prefix with 0z[10] if it's a numeric string and not already prefixed
        // We handle negatives by putting the prefix after the sign
        if (/^-?[\d./]+$/.test(str) && !str.includes("0z[10]")) {
            return str.replace(/^(-)?/, "$10z[10]");
        }

        return str;
    }

    /**
     * Format a value for display
     */
    formatValue(value) {
        // Handle undefined/null
        if (value === undefined || value === null) {
            return "undefined";
        }
        // Handle string type
        if (value && value.type === "string") {
            // Check if this string is a function reference
            const funcName = value.value;
            if (funcName && this.functions.has(funcName)) {
                const funcDef = this.functions.get(funcName);
                // Format as function definition
                if (funcDef.params && funcDef.body) {
                    const params = funcDef.params.join(", ");
                    return `${params} -> ${funcDef.body}`;
                }
            }
            return `"${value.value}"`;
        }
        if (value && value.type === "object") {
            // Format object as {Object}
            return "{Object}";
        }
        // Handle Oracle (function with yes property)
        if (typeof value === 'function' && value.yes) {
            return `[Oracle] yes: ${value.yes.toString()}`;
        }
        // Handle Promise (async results)
        if (value && typeof value.then === 'function') {
            return `[Promise - use await]`;
        }
        if (value && value.type === "sequence") {
            // Format sequence as [val1, val2, val3, ...]
            const formattedValues = value.values.map((v) => this.formatValue(v));
            if (formattedValues.length <= 10) {
                return `[${formattedValues.join(", ")}]`;
            } else {
                // For long sequences, show first few, ..., last few
                const start = formattedValues.slice(0, 3);
                const end = formattedValues.slice(-3);
                return `[${start.join(", ")}, ..., ${end.join(", ")}] (${formattedValues.length} values)`;
            }
        } else if (value instanceof RationalInterval) {
            return `${value.low.toString()}:${value.high.toString()}`;
        } else if (value instanceof Rational) {
            return value.toString();
        } else if (value instanceof Integer) {
            return value.value.toString();
        } else {
            return value.toString();
        }
    }

    /**
     * Convert a value to integer for iteration
     */
    toInteger(value) {
        if (value instanceof Integer) {
            return Number(value.value);
        } else if (value instanceof Rational) {
            if (value.denominator !== 1n) {
                throw new Error("Iterator bounds must be integers");
            }
            return Number(value.numerator);
        } else {
            throw new Error("Iterator bounds must be integers");
        }
    }

    /**
     * Restore a variable to its previous value
     */
    restoreVariable(variable, oldValue) {
        if (oldValue !== undefined) {
            this.variables.set(variable, oldValue);
        } else {
            this.variables.delete(variable);
        }
    }

    /**
     * Get all defined variables
     */
    getVariables() {
        return new Map(this.variables);
    }

    /**
     * Get all defined functions
     */
    getFunctions() {
        return new Map(this.functions);
    }

    /**
     * Clear all variables and functions
     */
    clear() {
        this.variables.clear();
        this.functions.clear();
    }

    /**
     * Set progress callback for long-running computations
     */
    setProgressCallback(callback) {
        this.progressCallback = callback;
    }

    /**
     * Set the map of custom base systems
     * @param {Map<number, BaseSystem>} customBases - Map of base number to BaseSystem
     */
    setCustomBases(customBases) {
        this.customBases = customBases;
    }

    /**
 * Freeze an expression by resolving static variables, snapshotting functions, and fixing numbers to base.
 * @param {string} expression - The expression string
 * @param {Set<string>} paramSet - Set of parameter names to preserve/prefix
 * @returns {string} - The frozen expression
 */
    freezeExpression(expression, paramSet) {
        // Context-aware processing to avoid replacing inside strings
        let result = "";
        let i = 0;
        let inString = false;

        // Tokenizer regex for variables and numbers (same as before)
        // We match at current position
        const varRegex = /^(?:@@[a-zA-Z0-9_]+@)?(?:@?[_a-zA-Z][a-zA-Z0-9_]*)/;
        const numRegex = /^(?:\d+[a-zA-Z0-9.]*|0[dxob][a-zA-Z0-9.]+)/;

        while (i < expression.length) {
            const char = expression[i];

            if (char === '"') {
                if (inString) {
                    // Check escaped
                    let backslashCount = 0;
                    let j = i - 1;
                    while (j >= 0 && expression[j] === '\\') {
                        backslashCount++;
                        j--;
                    }
                    if (backslashCount % 2 === 0) inString = false;
                } else {
                    inString = true;
                }
                result += char;
                i++;
                continue;
            }

            if (inString) {
                result += char;
                i++;
                continue;
            }

            // Outside string: Check for Variables or Numbers
            const tail = expression.substring(i);

            // Check Variable
            // Must ensure we are not in middle of word? 
            // Regex ^ matches start of tail.
            // Also need to check if previous char was identifier char to avoid matching suffix?
            // "var1" -> matches "var1".
            // "myvar" -> matches "myvar".
            // "3var" -> 3 matches number, var matches var?
            // "var" at i. Check prev char.
            const prevChar = i > 0 ? expression[i - 1] : " ";
            const isWordStart = /[^a-zA-Z0-9_@]/.test(prevChar);

            if (isWordStart) {
                const varMatch = tail.match(varRegex);
                if (varMatch) {
                    const fullMatch = varMatch[0];
                    const identifier = fullMatch; // assuming match is just identifier

                    // Logic from original freezeExpression
                    const normalize = (s) => s.startsWith("@") ? s.substring(1) : s;
                    const norm = normalize(identifier);
                    // Handle namespace @@Mod@Name -> Name is part after last @?
                    // normalize logic in original was: identifier.replace(/^@/, '')
                    // Let's stick to simple normalization
                    const simpleNorm = identifier.replace(/^@/, '');

                    // Check lambda param
                    // Missing: Lambda Check at start of expression? 
                    // Original had logic to detect "params ->". 
                    // That logic was global at start. We should do that first.
                    // But here we are continuously processing.

                    // Re-implement Lambda protection:
                    // We need 'protectedParams' passed in or calculated.
                    // If we do loop, we might miss the initial "params ->" detection if we don't do it upfront.

                    // Let's assume we pass strictly correct params. 
                    // But wait, the original logic detected Lambda params inside the body? 
                    // No, `expression` is the body? 
                    // "expression" argument.
                    // `freezeExpression(body, paramSet)`
                    // If `body` contains `x -> x+1`, `x` is protected.
                    // We need to detect strict lambdas.

                    // Let's revert to using `replace` but with a trick:
                    // Hide strings first, then process, then restore?
                    // That's safer and easier than full parser.

                    result += fullMatch; // Placeholder, see strategy below
                    i += fullMatch.length;
                    continue;
                }

                // Check Number
                const numMatch = tail.match(numRegex);
                if (numMatch) {
                    const fullMatch = numMatch[0];
                    // ... logic ...
                    result += fullMatch;
                    i += fullMatch.length;
                    continue;
                }
            }

            result += char;
            i++;
        }

        // RE-STRATEGY: 
        // Writing a full tokenizer here is error-prone.
        // Better Strategy: masking strings.
        // 1. Extract strings and replace with placeholders `__STR_0__`, `__STR_1__`.
        // 2. Run original logic.
        // 3. Restore strings.
        return this.freezeExpressionWithMasking(expression, paramSet);
    }

    freezeExpressionWithMasking(expression, paramSet) {
        const strings = [];
        let masked = "";
        let i = 0;
        let inString = false;
        let chunkStart = 0;

        while (i < expression.length) {
            if (expression[i] === '"') {
                // handle quote...
                if (!inString) {
                    masked += expression.substring(chunkStart, i);
                    inString = true;
                    chunkStart = i; // include quote
                } else {
                    // Check escaped
                    let backslashCount = 0;
                    let j = i - 1;
                    while (j >= chunkStart && expression[j] === '\\') {
                        backslashCount++;
                        j--;
                    }
                    if (backslashCount % 2 === 0) {
                        inString = false;
                        const strContent = expression.substring(chunkStart, i + 1);
                        const placeholder = `__STR_${strings.length}__`;
                        strings.push(strContent);
                        masked += placeholder;
                        chunkStart = i + 1;
                    }
                }
            }
            i++;
        }
        masked += expression.substring(chunkStart);

        // NOW RUN ORIGINAL LOGIC on 'masked'
        let staticExpr = masked;

        // Lambda Detection: Check if expression starts with "params ->"
        const lambdaMatch = staticExpr.match(/^\s*(?:\(?([a-zA-Z0-9_, ]+)\)?)\s*->/);
        const protectedParams = new Set();
        if (lambdaMatch) {
            const rawParams = lambdaMatch[1];
            rawParams.split(',').forEach(p => protectedParams.add(p.trim()));
        }

        const varRegex = /(?:^|[^a-zA-Z0-9_@])((?:@@[a-zA-Z0-9_]+@)?(?:@?[_a-zA-Z][a-zA-Z0-9_]*))/g;
        staticExpr = staticExpr.replace(varRegex, (fullMatch, identifier, offset, string) => {
            const prefix = fullMatch.substring(0, fullMatch.indexOf(identifier));
            const rawName = identifier.replace(/^@/, '');
            const norm = this.normalizeName(rawName);

            // Check placeholders
            if (identifier.startsWith("__STR_") && identifier.endsWith("__")) return fullMatch;

            if (protectedParams.has(norm)) return fullMatch;
            if (paramSet.has(norm)) {
                // Auto-prefix params if not already prefixed
                if (!identifier.startsWith('@')) return prefix + "@" + norm;
                return fullMatch;
            }
            if (norm.startsWith('_')) return fullMatch;

            if (this.functions.has(norm)) {
                if (norm.startsWith('_')) return fullMatch;
                const originalFunc = this.functions.get(norm);
                if (originalFunc.type === 'js') return fullMatch;

                // Snapshot
                const timestamp = Date.now().toString(36);
                const random = Math.random().toString(36).substring(2, 6);
                const snapshotName = `@@Static@${norm}_${timestamp}${random}`;
                const normalizedSnapshotName = this.normalizeName(snapshotName);
                const snapshotFunc = { ...originalFunc, type: 'def', doc: `[Snapshot] ${originalFunc.doc}` };
                this.functions.set(normalizedSnapshotName, snapshotFunc);
                return prefix + normalizedSnapshotName;
            }

            if (this.variables.has(norm)) {
                const val = this.variables.get(norm);
                return prefix + this.formatValueWithPrefix(val);
            }

            if (!norm.startsWith('_')) {
                // Warning or Error? Original threw error.
                // We kept error.
                throw new Error(`Undefined variable or function '${norm}' at definition time. Use '_${norm}' for dynamic resolution or define it first.`);
            }
            return fullMatch;
        });

        // Freeze Numbers
        const numRegex = /(?:^|[^a-zA-Z0-9_@])((?:0z\[\d+\])?[0-9][a-zA-Z0-9.]*|0[dxob][a-zA-Z0-9.]+)/g;
        staticExpr = staticExpr.replace(numRegex, (fullMatch, numStr, offset, string) => {
            // Check if part of placeholder?
            if (fullMatch.includes("__STR_")) return fullMatch;

            const prefix = fullMatch.substring(0, fullMatch.indexOf(numStr));
            try {
                const evalRes = this.evaluateExpression(numStr, new Map());
                if (evalRes.type !== 'error' && evalRes.result !== undefined) {
                    const val = evalRes.result;
                    const safeStr = this.formatValueWithPrefix(val);
                    const charAfter = string[offset + fullMatch.length];
                    let insertion = safeStr;
                    if (/[a-zA-Z0-9]/.test(charAfter)) insertion += " ";
                    return prefix + insertion;
                }
            } catch (e) { }
            return fullMatch;
        });

        // Restore Strings
        for (let k = 0; k < strings.length; k++) {
            staticExpr = staticExpr.replace(`__STR_${k}__`, strings[k]);
        }

        return staticExpr;
    }

    /**
     * Normalize a property name for case-insensitive lookup
     * Properties are case-insensitive except for first letter which determines type
     */
    normalizePropName(propName) {
        if (!propName) return propName;
        // Keep first letter case, lowercase the rest for case-insensitive matching
        return propName[0] + propName.slice(1).toLowerCase();
    }

    /**
     * Set a decoration property on a variable or function
     * Property names are case-insensitive but display case is preserved
     * @param {string} name - Variable or function name
     * @param {string} propName - Property name
     * @param {any} value - Property value
     */
    setDecoration(name, propName, value) {
        const normalizedName = this.normalizeName(name);
        const normalizedProp = this.normalizePropName(propName);
        if (!this.decorations.has(normalizedName)) {
            this.decorations.set(normalizedName, new Map());
        }
        // Store value with original display name
        this.decorations.get(normalizedName).set(normalizedProp, {
            value: value,
            displayName: propName
        });
    }

    /**
     * Get a decoration property from a variable or function
     * @param {string} name - Variable or function name
     * @param {string} propName - Property name
     * @returns {any} - Property value or undefined
     */
    getDecoration(name, propName) {
        const normalizedName = this.normalizeName(name);
        const normalizedProp = this.normalizePropName(propName);
        if (!this.decorations.has(normalizedName)) {
            return undefined;
        }
        const entry = this.decorations.get(normalizedName).get(normalizedProp);
        // Return just the value, not the wrapper
        return entry?.value;
    }

    /**
     * Check if a decoration property exists
     * @param {string} name - Variable or function name
     * @param {string} propName - Property name
     * @returns {boolean}
     */
    hasDecoration(name, propName) {
        const normalizedName = this.normalizeName(name);
        const normalizedProp = this.normalizePropName(propName);
        if (!this.decorations.has(normalizedName)) {
            return false;
        }
        return this.decorations.get(normalizedName).has(normalizedProp);
    }

    /**
     * Delete a decoration property
     * @param {string} name - Variable or function name
     * @param {string} propName - Property name
     * @returns {boolean} - True if deleted
     */
    deleteDecoration(name, propName) {
        const normalizedName = this.normalizeName(name);
        const normalizedProp = this.normalizePropName(propName);
        if (!this.decorations.has(normalizedName)) {
            return false;
        }
        return this.decorations.get(normalizedName).delete(normalizedProp);
    }

    /**
     * Get all decoration properties for a variable or function
     * Returns a Map with display names as keys
     * @param {string} name - Variable or function name
     * @returns {Map|undefined} - Map of property names to values
     */
    getDecorations(name) {
        const normalizedName = this.normalizeName(name);
        const rawMap = this.decorations.get(normalizedName);
        if (!rawMap) return undefined;

        // Convert to display format
        const result = new Map();
        for (const [normalizedKey, entry] of rawMap) {
            result.set(entry.displayName, entry.value);
        }
        return result;
    }

    /**
     * Get all decoration property names for a variable or function
     * @param {string} name - Variable or function name
     * @returns {string[]} - Array of property names (display names)
     */
    getDecorationKeys(name) {
        const normalizedName = this.normalizeName(name);
        const rawMap = this.decorations.get(normalizedName);
        if (!rawMap) {
            return [];
        }
        return Array.from(rawMap.values()).map(entry => entry.displayName);
    }
}
