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
        const numberPattern = new RegExp(
            `\\b(-?[${validChars}]+(?:\\.[${validChars}]+)?(?:\\.\\.[${validChars}]+(?:\\/[${validChars}]+)?)?(?:\\/[${validChars}]+)?(?:\\_\\^-?[${validChars}]+)?)\\b(?!\\s*\\[)`,
            "g",
        );

        return expression.replace(numberPattern, (match) => {
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
        const trimmed = input.trim();

        // Check for assignment: x = expression
        const assignmentMatch = trimmed.match(/^([a-zA-Z])\s*=\s*(.+)$/);
        if (assignmentMatch) {
            const [, varName, expression] = assignmentMatch;
            return this.handleAssignment(varName, expression);
        }

        // Check for function definition: P[x,y] = expression
        const functionMatch = trimmed.match(
            /^([A-Z])\[([a-zA-Z,\s]+)\]\s*=\s*(.+)$/,
        );
        if (functionMatch) {
            const [, funcName, paramStr, expression] = functionMatch;
            const params = paramStr
                .split(",")
                .map((p) => p.trim())
                .filter((p) => p.length === 1);
            return this.handleFunctionDefinition(funcName, params, expression);
        }

        // Check for function call: P(1,2)
        const callMatch = trimmed.match(/^([A-Z])\(([^)]*)\)$/);
        if (callMatch) {
            const [, funcName, argsStr] = callMatch;
            return this.handleFunctionCall(funcName, argsStr);
        }

        // Check for special functions: SUM[i](expr, start, end, increment?)
        const specialMatch = trimmed.match(
            /^(SUM|PROD|SEQ)\[([a-zA-Z])\]\(([^,]+),\s*([^,]+),\s*([^,]+)(?:,\s*([^)]+))?\)$/,
        );
        if (specialMatch) {
            const [, keyword, variable, expression, start, end, increment] =
                specialMatch;
            return this.handleSpecialFunction(
                keyword,
                variable,
                expression,
                start,
                end,
                increment || "1",
            );
        }

        // Regular expression evaluation with variable substitution
        return this.evaluateExpression(trimmed);
    }

    /**
     * Handle variable assignment
     */
    handleAssignment(varName, expression) {
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
    handleFunctionDefinition(funcName, params, expression) {
        // Validate parameters are single characters
        for (const param of params) {
            if (param.length !== 1 || !/[a-zA-Z]/.test(param)) {
                return {
                    type: "error",
                    message: `Function parameters must be single letters, got: ${param}`,
                };
            }
        }

        this.functions.set(funcName, { params, expression });
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
            const result = this.evaluateExpression(func.expression);

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
     * Evaluate an expression with variable substitution
     */
    evaluateExpression(expression) {
        try {
            // Check for special functions first, before variable substitution
            const specialMatch = expression.match(
                /^(SUM|PROD|SEQ)\[([a-zA-Z])\]\(([^,]+),\s*([^,]+),\s*([^,]+)(?:,\s*([^)]+))?\)$/,
            );
            if (specialMatch) {
                const [, keyword, variable, expr, start, end, increment] = specialMatch;
                return this.handleSpecialFunction(
                    keyword,
                    variable,
                    expr,
                    start,
                    end,
                    increment || "1",
                );
            }

            // Substitute variables in the expression
            let substituted = expression;
            for (const [varName, value] of this.variables) {
                // Replace variable with its value, being careful about word boundaries
                const pattern = new RegExp(`\\b${varName}\\b`, "g");
                substituted = substituted.replace(
                    pattern,
                    `(${this.formatValue(value)})`,
                );
            }

            // Preprocess for input base conversion
            const preprocessed = this.preprocessExpression(substituted);

            // Parse and evaluate
            const result = Parser.parse(preprocessed, {
                typeAware: true,
                customBases: this.customBases,
                inputBase: this.inputBase
            });
            return {
                type: "expression",
                result: result,
            };
        } catch (error) {
            return {
                type: "error",
                message: error.message,
            };
        }
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
