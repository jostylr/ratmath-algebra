/**
 * var.js
 *
 * Variable management and mini-language features for the calculator.
 * Supports single-character variables, function definitions, and special functions like SUM, PROD, SEQ.
 */

import { Rational, RationalInterval, Integer, BaseSystem } from "@ratmath/core";
import { Parser } from "@ratmath/parser";

export class VariableManager {
    constructor() {
        this.variables = new Map(); // Store single-character variables
        this.functions = new Map(); // Store function definitions
        this.modules = new Map();   // Store loaded modules { name: string, content: object }
        this.inputBase = null; // Base system for interpreting numbers without explicit base notation
        this.customBases = new Map(); // Store custom base definitions

        // Regex patterns for validation
        // Updaed to support namespacing: @@Module@Name
        // Variable: starts with lowercase or underscore, optional @@Mod@ prefix
        this.variablePattern = /^(?:@@[a-zA-Z0-9_]+@)?(?:@?([_a-z][a-zA-Z0-9_]*))$/;
        // Function: starts with uppercase, optional @@Mod@ prefix
        this.functionPattern = /^(?:@@[a-zA-Z0-9_]+@)?(?:@?([A-Z][a-zA-Z0-9_]*))$/;
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
        // Normalize: strip leading @
        const normalizedName = name.startsWith("@") ? name.substring(1) : name;
        this.variables.set(normalizedName, value);
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
        this.functions.set(name, { params, body, doc, type: 'def', defaults });
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
        this.functions.set(name, { type: 'js', handler, params, doc });
    }

    /**
     * Get help/documentation for a function
     * @param {string} [name] - Function name (optional)
     * @returns {string} - Help text
     */
    getHelp(name) {
        if (name) {
            const normalized = name.startsWith("@@") ? name : (name.startsWith("@") ? name.substring(1) : name);
            if (this.functions.has(normalized)) {
                const f = this.functions.get(normalized);
                const sig = `${normalized}(${f.params.join(", ")})`;
                return `${sig}\n${f.doc || "No documentation available."}`;
            }
            return `Function '${name}' not found.`;
        }

        // List all functions with short doc
        const entries = [];
        for (const [fname, f] of this.functions) {
            let snippet = f.doc ? f.doc.split('\n')[0] : "";
            if (snippet.length > 50) snippet = snippet.substring(0, 47) + "...";
            entries.push(`${fname}(${f.params.join(",")}) - ${snippet}`);
        }
        return `Available Functions:\n${entries.join('\n')}`;
    }

    /**
     * Load a module into the current namespace
     * @param {string} moduleName - Name of the module (e.g. "Core")
     * @param {object} scope - Object containing vars and functions to load
     */
    loadModule(moduleName, scope) {
        const prefix = `@@${moduleName}@`;

        // Register functions
        if (scope.functions) {
            for (const [name, def] of Object.entries(scope.functions)) {
                const qualifiedName = `${prefix}${name}`;
                this.functions.set(qualifiedName, { ...def });
                // Also alias simple name if not conflicting?
                // User said: "LOAD @@Module to put all the functions and variables in the Module in the current active space"
                // This implies making them available WITHOUT prefix too?
                // "Also a command LOAD @@Module to put all ... in the current active space"
                // And "namespacing convention of @@Module@Func ... so @@ is for Module name"
                // I'll assume LOAD makes them available as `Name` (overwriting?) AND `@@Module@Name` is always available if module is known?
                // Or maybe LOAD copies them to main namespace.

                // Let's implement LOAD as: Import scoped items into main namespace.
                // The @@Module@Name convention might be for storage or direct access?
                // If I store them as `@@Module@Name`, user has to type that. 
                // LOAD matches "using namespace" in C++.

                // Let's store as fully qualified, and also create aliases in main map.
                this.functions.set(name, { ...def, isImported: true, module: moduleName });
            }
        }

        // Register variables
        if (scope.variables) {
            for (const [name, val] of Object.entries(scope.variables)) {
                const qualifiedName = `${prefix}${name}`;
                this.variables.set(qualifiedName, val);
                this.variables.set(name, val);
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
            `\\b(-?[${validChars}0-9a-zA-Z]+(?:\\.[${validChars}0-9a-zA-Z]*)?(?:\\.\\.[${validChars}0-9a-zA-Z]+(?:\\/[${validChars}0-9a-zA-Z]+)?)?(?:\\/[${validChars}0-9a-zA-Z]+)?(?:\\_\\^-?[${validChars}0-9a-zA-Z]+)?)(?:\\[([^\\]]+)\\](?:[Ee][+-]?[${validChars}0-9a-zA-Z]+|\\_\\^-?[${validChars}0-9a-zA-Z]+)?|\\b(?!\\s*\\[))`,
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

                // Helper to format with 0d prefix handling negatives
                const toPrefixed0d = (s) => s.startsWith("-") ? `-0d${s.substring(1)}` : `0d${s}`;

                // Check for Scientific Notation _^
                if (match.includes("_^")) {
                    const [basePart, expPart] = match.split("_^");
                    const baseValStr = parseToRational(basePart);
                    const expValStr = parseToRational(expPart);
                    // Construct expression: (Base) * (SystemBase) ^ (Exp)
                    return `(${toPrefixed0d(baseValStr)}) * (${toPrefixed0d(this.inputBase.base.toString())}) ^ (${toPrefixed0d(expValStr)})`;
                }

                // Standard Number
                const valStr = parseToRational(match);
                // Return with 0d prefix to strip context
                return toPrefixed0d(valStr);

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

            // 1. Function Definition: Name(args) -> body
            // Matches: FuncName(args) -> body
            // FuncName must be generic word or @word to match properly (validation inside handler)
            const funcDefMatch = trimmed.match(/^(@?[_a-zA-Z][a-zA-Z0-9_]*)\s*\(([^)]*)\)\s*->\s*(.+)$/);
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

            // 3. Variable Assignment: Name = Expression
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
                const normalizedName = name.startsWith("@") ? name.substring(1) : name;

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
                        throw new Error(`Ambiguous reference '${name}'. Use @${name} for function or explicit base prefix (e.g. 0D${name} or 0x${name}) for number.`);
                    }

                    const f = this.functions.get(normalizedName);
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
                const normSource = sourceName.startsWith("@") ? sourceName.substring(1) : sourceName;
                const normTarget = varName.startsWith("@") ? varName.substring(1) : varName;

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

            // 2. Check for List/Sequence Assignment
            // Evaluate expression to see if it's a list
            try {
                const result = this.evaluateExpression(expression);
                if (result.type !== "error" && result.result && result.result.type === "sequence") {
                    const normTarget = varName.startsWith("@") ? varName.substring(1) : varName;
                    // Store as List Accessor Function
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

            // For sequences, store the last value but show the assignment
            let valueToStore = result.result;
            let displayValue = result.result;

            if (result.result && result.result.type === "sequence") {
                valueToStore = result.result.lastValue;
                displayValue = result.result;
            }

            this.variables.set(varName, valueToStore);

            // For sequences, show assignment differently
            let message;
            if (result.result && result.result.type === "sequence") {
                message = `${varName} = ${this.formatValue(valueToStore)} (assigned last value of ${this.formatValue(displayValue)})`;
            } else {
                message = `${varName} = ${this.formatValue(displayValue)}`;
            }

            return {
                type: "assignment",
                result: displayValue,
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
        // 1. Numbers: Convert to 0d decimal literal to make them base-independent
        // 2. Variables: If not in params and not underscore prefixed, capture current value (freeze)
        // 3. Defaults: Freeze default values too

        const staticBody = this.freezeExpression(body, paramSet);

        // Freeze Defaults
        const staticDefaults = {};
        for (const [key, val] of Object.entries(defaults)) {
            staticDefaults[key] = this.freezeExpression(val, paramSet);
        }

        this.functions.set(funcName, { params, body: staticBody, type: 'def', doc: doc || `User defined function: ${body}`, defaults: staticDefaults });
        return {
            type: "function",
            result: null,
            message: `Function ${funcName}[${params.join(",")}] defined`,
        };
    }

    /**
     * Handle function call
     */
    handleFunctionCall(funcName, argsStr) {
        if (!this.functions.has(funcName)) {
            return {
                type: "error",
                message: `Function ${funcName} not defined`,
            };
        }

        const func = this.functions.get(funcName);

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

                // CHECK FOR EXPLICIT LAMBDA: "var -> expr"
                const lambdaMatch = argRaw.match(/^([a-zA-Z][a-zA-Z0-9_]*)\s*->\s*(.+)$/);

                if (lambdaMatch) {
                    if (!isParamFunction) {
                        // Case: Parameter is lowercase (value), but passed lambda.
                        throw new Error(`Argument mismatch for '${cleanParamName}': Expected value (compatible with lowercase), got Lambda function.`);
                    }

                    // Create Anonymous Function
                    const [, lambdaParam, lambdaBody] = lambdaMatch;
                    // Use namespaced Format: @@Anon@<Timestamp>_<Random>
                    // This satisfies the functionCallRegex logic.
                    const anonName = `@@Anon@${Date.now()}_${Math.floor(Math.random() * 1000)}`;

                    this.functions.set(anonName, {
                        params: [lambdaParam.trim()],
                        body: lambdaBody.trim(),
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
                        // Normalize: strip @ but respect @@
                        const norm = trimmed.startsWith("@@") ? trimmed : (trimmed.startsWith("@") ? trimmed.substring(1) : trimmed);
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

            // Helper to lookup variable in chain
            // Note: lookup logic is embedded in Variable Substitution section below or logic uses map merge for regex
            // But for explicit checks we need helpers
            const hasVar = (name) => {
                for (const scope of scopeChain) {
                    if (scope.has(name)) return true;
                    // Fallback for @ prefix
                    if (name.startsWith('@') && scope.has(name.substring(1))) return true;
                }
                if (this.variables.has(name)) return true;
                if (name.startsWith('@') && this.variables.has(name.substring(1))) return true;
                return false;
            };
            const getVar = (name) => {
                for (const scope of scopeChain) {
                    if (scope.has(name)) return scope.get(name);
                    // Fallback for @ prefix
                    if (name.startsWith('@') && scope.has(name.substring(1))) return scope.get(name.substring(1));
                }
                if (this.variables.has(name)) return this.variables.get(name);
                if (name.startsWith('@') && this.variables.has(name.substring(1))) return this.variables.get(name.substring(1));
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
                else if (upperCommand === "DEC" || command === "0d") tempBase = BaseSystem.DECIMAL;
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

            // Function Call Substitution
            // Matches: Name(args)
            const functionCallRegex = /(?:^|[^a-zA-Z0-9_@])((?:@@[a-zA-Z0-9_]+@)?(?:@?[_a-zA-Z][a-zA-Z0-9_]*))\s*\(/g;
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
                    // Normalize function name
                    const normalizedFuncName = funcName.startsWith("@@") ? funcName : (funcName.startsWith("@") ? funcName.substring(1) : funcName);
                    let funcDef = this.functions.get(normalizedFuncName);

                    // Alias Lookup in Scope Chain
                    if (!funcDef) {
                        const aliasVal = getVar(normalizedFuncName);
                        if (typeof aliasVal === 'string') {
                            const aliasNorm = aliasVal.startsWith("@@") ? aliasVal : (aliasVal.startsWith("@") ? aliasVal.substring(1) : aliasVal);
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
                            for (const arg of args) {
                                const lambdaMatch = arg.match(/^([a-zA-Z][a-zA-Z0-9_]*)\s*->\s*(.+)$/);
                                if (lambdaMatch) {
                                    const [, lParam, lBody] = lambdaMatch;
                                    const anonName = `@@Anon@Lambda_${Date.now()}_${Math.floor(Math.random() * 1000)}`;
                                    this.functions.set(anonName, {
                                        params: [lParam.trim()],
                                        body: lBody.trim(),
                                        type: 'def',
                                        doc: 'Anonymous Lambda'
                                    });
                                    argValues.push(anonName);
                                } else if (arg.trim() === '') {
                                    argValues.push(undefined);
                                } else {
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

                        const resultStr = this.formatValueWithPrefix(resultVal);

                        substitutedFunctions = substitutedFunctions.substring(0, startIndex) +
                            resultStr +
                            substitutedFunctions.substring(closeParenIndex + 1);

                        functionCallRegex.lastIndex = 0;
                        continue;
                    }
                }
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

                    // Match identifier from here
                    const tail = substitutedFunctions.substring(i);
                    const match = tail.match(/^((?:@@[a-zA-Z0-9_]+@)?(?:@?[_a-zA-Z][a-zA-Z0-9_]*))/);

                    if (match) {
                        const token = match[1];
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
                                        message: `Ambiguous reference '${token}'. It is both a variable and a valid number in ${this.inputBase.name}. Use @${token} for variable or 0D${token} (or 0${BaseSystem.getPrefixForSystem(this.inputBase) || 'd'}${token}) for number.`
                                    };
                                }
                            }

                            const val = getVar(token);
                            const s = this.formatValueWithPrefix(val);
                            finalExpr += s;
                            i += token.length;
                            continue;
                        }
                        // Check if known function (used as value)
                        else if (this.functions.has(token)) { // && !token.includes('@') check disabled strictly
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

            const specialMatch = preprocessed.match(
                /^(SUM|PROD|SEQ)\[([a-zA-Z])\]\(([^,]+),\s*([^,]+),\s*([^,]+)(?:,\s*([^)]+))?\)$/,
            );
            if (specialMatch) {
                const [, keyword, variable, expr, start, end, increment] = specialMatch;
                return this.handleSpecialFunction(keyword, variable, expr, start, end, increment || "1");
            }

            let result;
            try {
                result = Parser.parse(preprocessed, {
                    typeAware: true,
                    customBases: this.customBases,
                    inputBase: this.inputBase
                });
            } catch (parseError) {
                // Silenced debug log unless critical
                // console.error("evaluateExpression Parse Error on:", preprocessed);
                // console.error("Original error:", parseError.message);

                // HOC fallback
                const trimmed = preprocessed.trim();
                const tokens = trimmed.split(/[^a-zA-Z0-9@_]/).filter(t => t.length > 0);
                for (const token of tokens) {
                    const rawName = token.startsWith("@@") ? token : (token.startsWith("@") ? token.substring(1) : token);
                    if (this.functions.has(rawName)) {
                        if (tokens.length > 1 || trimmed.includes("(") || trimmed.includes(")")) {
                            throw new Error(`Function '${rawName}' cannot be used as a value in this context`);
                        }
                        return { type: "expression", result: rawName };
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

        if (value.type === "sequence") {
            const formatted = value.values.map(v => this.formatValueWithPrefix(v));
            return `[${formatted.join(", ")}]`;
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

        // Prefix with 0d if it's a numeric string and not already prefixed
        // We handle negatives by putting the prefix after the sign
        if (/^-?[\d./]+$/.test(str) && !str.includes("0d")) {
            return str.replace(/^(-)?/, "$10d");
        }

        return str;
    }

    /**
     * Format a value for display
     */
    formatValue(value) {
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
            const norm = identifier.replace(/^@/, '');

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
                const snapshotFunc = { ...originalFunc, type: 'def', doc: `[Snapshot] ${originalFunc.doc}` };
                this.functions.set(snapshotName, snapshotFunc);
                return prefix + snapshotName;
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
        const numRegex = /(?:^|[^a-zA-Z0-9_@])(\d+[a-zA-Z0-9.]*|0[dxob][a-zA-Z0-9.]+)/g;
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
}
