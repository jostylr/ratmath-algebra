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
        this.inputBase = null; // Base system for interpreting numbers without explicit base notation
        this.customBases = new Map(); // Store custom base definitions

        // Regex patterns for validation
        this.variablePattern = /^(?:@?([a-z][a-zA-Z0-9]*))$/;
        this.functionPattern = /^(?:@?([A-Z][a-zA-Z0-9]*))$/;
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
            throw new Error(`Invalid variable name '${name}'. Variables must start with a lowercase letter or @lowercase.`);
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
     */
    defineFunction(name, params, body) {
        if (!this.functionPattern.test(name)) {
            // Check if it looks like a variable name
            if (this.variablePattern.test(name)) {
                throw new Error(`Invalid function name '${name}'. Function definitions must use names starting with an Uppercase letter or @Uppercase.`);
            }
            throw new Error(`Invalid function name '${name}'. Functions must start with an Uppercase letter or @Uppercase.`);
        }
        this.functions.set(name, { params, body });
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
            `\\b(-?[${validChars}]+(?:\\.[${validChars}]+)?(?:\\.\\.[${validChars}]+(?:\\/[${validChars}]+)?)?(?:\\/[${validChars}]+)?(?:\\_\\^-?[${validChars}]+)?)(?:\\[([^\\]]+)\\](?:[Ee][+-]?[${validChars}]+|\\_\\^-?[${validChars}]+)?|\\b(?!\\s*\\[))`,
            "g",
        );

        return expression.replace(numberPattern, (match, baseValue, uncertainty) => {
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
            const funcDefMatch = trimmed.match(/^(@?[a-zA-Z][a-zA-Z0-9]*)\s*\(([^)]*)\)\s*->\s*(.+)$/);
            if (funcDefMatch) {
                const [, funcName, paramStr, body] = funcDefMatch;
                const params = paramStr.split(",").map(p => p.trim()).filter(p => p);
                return this.handleFunctionDefinition(funcName, params, body);
            }

            // 2. Assignment Style Definition: Name = (args) -> body  OR  Name = arg -> body
            // Matches: Name = ... -> ...
            const arrowAssignMatch = trimmed.match(/^(@?[a-zA-Z][a-zA-Z0-9]*)\s*=\s*(?:(?:\(([^)]*)\))|([a-zA-Z][a-zA-Z0-9]*))\s*->\s*(.+)$/);
            if (arrowAssignMatch) {
                const [, name, paramsInParens, singleParam, body] = arrowAssignMatch;
                const params = (paramsInParens !== undefined ? paramsInParens : singleParam)
                    .split(",").map(p => p.trim()).filter(p => p);
                // This is a function definition disguised as assignment
                return this.handleFunctionDefinition(name, params, body);
            }

            // 3. Variable Assignment: Name = Expression
            // Matches: Name = ... (but NOT -> as that's handled above)
            // We match generic identifier, validation of case happens in handleAssignment
            const assignmentMatch = trimmed.match(/^(@?[a-zA-Z][a-zA-Z0-9]*)\s*=\s*(.+)$/);
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
            return this.evaluateExpression(trimmed);
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
            // Can only be assigned if RHS is an existing FUNCTION (Aliasing)
            // Check if expression is a simple identifier (potential function alias)
            const aliasMatch = expression.trim().match(/^(@?[A-Z][a-zA-Z0-9]*)$/);
            if (aliasMatch) {
                const sourceName = aliasMatch[1];
                const normSource = sourceName.startsWith("@") ? sourceName.substring(1) : sourceName;
                const normTarget = varName.startsWith("@") ? varName.substring(1) : varName;

                if (this.functions.has(normSource)) {
                    // Perform Aliasing (Copy Definition)
                    const sourceDef = this.functions.get(normSource);
                    this.functions.set(normTarget, { ...sourceDef }); // Copy params/body
                    return {
                        type: "function",
                        result: null,
                        message: `Function ${normTarget} defined as alias of ${normSource}`
                    };
                }
            }

            // If not a valid alias, reject assignment
            // "A = 4" falls here.
            return {
                type: "error",
                message: `Function names (starting with Uppercase) cannot be assigned values directly. To define a function, use '->' syntax or alias an existing function.`
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
    handleFunctionDefinition(funcName, params, body) {
        // Validate parameters are single characters
        for (const param of params) {
            if (param.length !== 1 || !/[a-zA-Z]/.test(param)) {
                return {
                    type: "error",
                    message: `Function parameters must be single letters, got: ${param}`,
                };
            }
        }

        this.functions.set(funcName, { params, body });
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
        const args = argsStr.trim() ? argsStr.split(",").map((s) => s.trim()) : [];

        if (args.length !== func.params.length) {
            return {
                type: "error",
                message: `Function ${funcName} expects ${func.params.length} arguments, got ${args.length}`,
            };
        }

        try {
            // Evaluate arguments
            const argValues = [];
            for (const arg of args) {
                const result = this.evaluateExpression(arg);
                if (result.type === "error") {
                    return result;
                }
                argValues.push(result.result);
            }

            // Create temporary variable bindings
            const oldValues = new Map();
            for (let i = 0; i < func.params.length; i++) {
                const param = func.params[i];
                if (this.variables.has(param)) {
                    oldValues.set(param, this.variables.get(param));
                }
                this.variables.set(param, argValues[i]);
            }

            // Evaluate function expression
            const result = this.evaluateExpression(func.body);

            // Restore old variable values
            for (const [param, oldValue] of oldValues) {
                this.variables.set(param, oldValue);
            }
            for (const param of func.params) {
                if (!oldValues.has(param)) {
                    this.variables.delete(param);
                }
            }

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
    evaluateExpression(expression, localScope = new Map()) {
        try {
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
                // Note: 0D as a command is handled by falling through (stays as default base)
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
                        return this.evaluateExpression(rest, localScope);
                    } finally {
                        this.setInputBase(originalBase);
                    }
                }
            }

            let substitutedFunctions = expression;

            // Function Call Substitution
            const functionCallRegex = /(?:^|[^a-zA-Z0-9_@])((?:@?[A-Z][a-zA-Z0-9]*))\s*\(/g;
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
                for (let i = openParenIndex + 1; i < substitutedFunctions.length; i++) {
                    if (substitutedFunctions[i] === '(') depth++;
                    else if (substitutedFunctions[i] === ')') depth--;
                    if (depth === 0) {
                        closeParenIndex = i;
                        break;
                    }
                }

                if (closeParenIndex !== -1) {
                    const argsStr = substitutedFunctions.substring(openParenIndex + 1, closeParenIndex);
                    // Normalize function name for lookup (strip @)
                    const normalizedFuncName = funcName.startsWith("@") ? funcName.substring(1) : funcName;
                    let funcDef = this.functions.get(normalizedFuncName);

                    // HOC Logic: If not found, check if it's a local variable (param) that holds a function name
                    if (!funcDef && localScope.has(normalizedFuncName)) {
                        const possibleAlias = localScope.get(normalizedFuncName);
                        if (typeof possibleAlias === 'string') {
                            const aliasNorm = possibleAlias.startsWith("@") ? possibleAlias.substring(1) : possibleAlias;
                            if (this.functions.has(aliasNorm)) {
                                funcDef = this.functions.get(aliasNorm);
                            }
                        }
                    }

                    if (funcDef) {
                        // Parse arguments with nested parenthesis support
                        const args = [];
                        let currentArg = "";
                        let argDepth = 0;
                        for (let i = 0; i < argsStr.length; i++) {
                            const char = argsStr[i];
                            if (char === '(' || char === '[' || char === '{') argDepth++;
                            else if (char === ')' || char === ']' || char === '}') argDepth--;

                            if (char === ',' && argDepth === 0) {
                                args.push(currentArg.trim());
                                currentArg = "";
                            } else {
                                currentArg += char;
                            }
                        }
                        if (currentArg.trim() !== "") args.push(currentArg.trim());

                        if (args.length !== funcDef.params.length) {
                            throw new Error(`Function '${funcName}' expects ${funcDef.params.length} arguments, got ${args.length}`);
                        }

                        const callBindScope = new Map();
                        for (let i = 0; i < funcDef.params.length; i++) {
                            const argResult = this.evaluateExpression(args[i], localScope);
                            if (argResult.type === "error") throw new Error(argResult.message);
                            callBindScope.set(funcDef.params[i], argResult.result);
                        }

                        const bodyResult = this.evaluateExpression(funcDef.body, callBindScope);
                        if (bodyResult.type === "error") throw new Error(bodyResult.message);

                        const resultStr = this.formatValueWithPrefix(bodyResult.result);

                        substitutedFunctions = substitutedFunctions.substring(0, startIndex) +
                            resultStr +
                            substitutedFunctions.substring(closeParenIndex + 1);

                        functionCallRegex.lastIndex = 0;
                        continue;
                    }
                }
            }

            // Variable Substitution
            let substituted = substitutedFunctions;

            // Build map of all active variables (global + local)
            const allVars = new Map([...this.variables, ...localScope]);

            // We need to identify tokens that look like variables in the expression.
            // A potential variable token matches [a-zA-Z][a-zA-Z0-9]* or @[a-zA-Z][a-zA-Z0-9]*
            // We'll iterate through matches and make substitution decisions based on strictness rules.

            substituted = substituted.replace(/(^|[^a-zA-Z0-9])(@?)([a-zA-Z][a-zA-Z0-9]*)/g, (match, prefixChar, atSign, name) => {
                // Unified Identity: variables and functions
                // Check Variables
                const normalizedName = name;
                const isVar = allVars.has(normalizedName);
                const isFunc = this.functions.has(normalizedName);
                const hasPrefix = atSign === "@";

                if (!isVar && !isFunc) {
                    // Not a known identifier, leave it alone.
                    return match;
                }

                let valStr;
                if (isVar) {
                    const value = allVars.get(normalizedName);
                    valStr = this.formatValueWithPrefix(value);
                } else if (isFunc) {
                    // Function used as value (not called with parens)
                    // 1. Check Ambiguity first (Function vs Digit)
                    let isDigit = false;
                    if (this.inputBase) {
                        const validChars = this.inputBase.base > 10
                            ? this.inputBase.characters.concat(this.inputBase.characters.filter(c => /[a-z]/.test(c)).map(c => c.toUpperCase()))
                            : this.inputBase.characters;
                        isDigit = [...name].every(c => validChars.includes(c));
                    }

                    if (isDigit && !hasPrefix) {
                        throw new Error(`Ambiguous reference '${name}'. Use @${name} for function or explicit base prefix (e.g. 0D${name} or 0x${name}) for number.`);
                    }

                    // 2. Allow Function as Value (strictness relaxed for HOC)
                    valStr = normalizedName;
                }

                // Strictness Logic:
                // 1. Check if 'name' is a valid digit
                let isDigit = false;
                if (this.inputBase) {
                    const validChars = this.inputBase.base > 10
                        ? this.inputBase.characters.concat(this.inputBase.characters.filter(c => /[a-z]/.test(c)).map(c => c.toUpperCase()))
                        : this.inputBase.characters;
                    isDigit = [...name].every(c => validChars.includes(c));
                }

                if (hasPrefix) {
                    // Accessor usage (@a). Always substitute.
                    if (isFunc) return `${prefixChar}${valStr}`;
                    return `${prefixChar}(${valStr})`;
                } else {
                    // No prefix usage (a).
                    if (isDigit) {
                        // Ambiguous! defined variable/function AND valid number.
                        // STRICTNESS: Throw Error.
                        const type = isVar ? "variable" : "function";
                        throw new Error(`Ambiguous reference '${name}'. Use @${name} for ${type} or explicit base prefix (e.g. 0D${name} or 0x${name}) for number.`);
                    } else {
                        // Not a digit (or base doesn't support letters), so it's safe to substitute.
                        if (isFunc) return `${prefixChar}${valStr}`;
                        return `${prefixChar}(${valStr})`;
                    }
                }
            });

            const preprocessed = this.preprocessExpression(substituted);

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
                const trimmed = preprocessed.trim();
                const rawName = trimmed.startsWith("@") ? trimmed.substring(1) : trimmed;
                if (this.functions.has(rawName)) {
                    return { type: "expression", result: rawName };
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
}
