/**
 * Package Registry for ratmath
 * 
 * Defines available packages, their metadata, dependencies, and help information.
 * Used by the LOAD command and HELP system in calc and webcalc.
 */

export const PackageRegistry = {
    // Real number approximations
    reals: {
        name: "Reals",
        description: "Real number approximations using rational intervals",
        requires: [],
        functions: ["PI", "E", "Exp", "Ln", "Log", "Sin", "Cos", "Tan", "Arcsin", "Arccos", "Arctan", "Root", "Pow"],
        help: `
Reals Package - Real Number Approximations

This package provides transcendental functions computed via rational interval
arithmetic. Results are returned as rational intervals that bound the true value.

CONSTANTS:
  PI([precision])     - Returns π ≈ 3.14159...
  E([precision])      - Returns e ≈ 2.71828...

EXPONENTIAL & LOGARITHM:
  Exp(x, [precision]) - Computes e^x
  Ln(x, [precision])  - Natural logarithm of x
  Log(x, base, [prec])- Logarithm of x in given base

TRIGONOMETRIC (radians):
  Sin(x, [precision]) - Sine of x
  Cos(x, [precision]) - Cosine of x
  Tan(x, [precision]) - Tangent of x

INVERSE TRIGONOMETRIC:
  Arcsin(x, [prec])   - Inverse sine (|x| ≤ 1)
  Arccos(x, [prec])   - Inverse cosine (|x| ≤ 1)
  Arctan(x, [prec])   - Inverse tangent

ROOTS & POWERS:
  Root(q, n, [prec])  - nth root of q
  Pow(base, exp, [p]) - base^exponent (fractional exponents)

PRECISION:
  All functions accept an optional precision parameter.
  Set _precision variable to change default (e.g., _precision = 1/1000000).
`
    },

    // Units package (stub for now)
    units: {
        name: "Units",
        description: "Physical unit conversions and dimensional analysis",
        requires: [],
        functions: [],
        help: `
Units Package - Physical Unit Conversions

This package provides unit conversions and dimensional analysis.
(Currently in development)

Planned features:
- Length, mass, time, temperature conversions
- Compound units (velocity, force, energy)
- User-defined unit systems
`
    },

    // Statistics package (stub)
    stats: {
        name: "Stats",
        description: "Statistical functions and distributions",
        requires: [],
        functions: [],
        help: `
Stats Package - Statistical Functions

This package provides statistical computations.
(Currently in development)

Planned features:
- Mean, median, mode, variance, std deviation
- Probability distributions
- Regression analysis
`
    },

    // Oracles package
    oracles: {
        name: "Oracles",
        description: "Oracle-based exact real arithmetic with lazy evaluation",
        requires: [],
        functions: ["Oracle", "OracleAdd", "OracleSub", "OracleMul", "OracleDiv", "OracleNeg", 
                    "Narrow", "OracleYes", "Sqrt", "NRoot", "CFSqrt2", "CFE", "CFPhi", 
                    "OracleFromCF", "Convergent", "Ask", "Estimate"],
        help: `
Oracles Package - Exact Real Arithmetic

Represents real numbers as "oracles" - functions that can refine
rational interval bounds to arbitrary precision on demand.

CREATING ORACLES:
  Oracle(x)              - Create oracle from Rational/RationalInterval
  Sqrt(x)                - Square root oracle (Newton's method)
  NRoot(x, n)            - nth root oracle
  CFSqrt2()              - Oracle for sqrt(2) from continued fraction
  CFE()                  - Oracle for e from continued fraction
  CFPhi()                - Oracle for golden ratio
  OracleFromCF(terms)    - Oracle from CF term array

ARITHMETIC (auto-converts Rational to Oracle):
  OracleAdd(a, b)        - Add oracles
  OracleSub(a, b)        - Subtract oracles
  OracleMul(a, b)        - Multiply oracles  
  OracleDiv(a, b)        - Divide oracles
  OracleNeg(a)           - Negate oracle

INSPECTION:
  OracleYes(oracle)      - Get current yes-interval
  Narrow(oracle, prec)   - Refine to precision, returns interval
  Convergent(cf, n)      - nth convergent of CF stream
  Ask(oracle, interval)  - Returns 1 if oracle in interval, 0 otherwise
  Estimate(oracle, prec) - Decimal estimate (default prec: 0.01 or _precision)

Note: Narrow, Ask, Estimate return Promises (async).
`
    },

    // Geometry package (stub)
    geometry: {
        name: "Geometry",
        description: "Geometric primitives and computations",
        requires: ["reals"],
        functions: [],
        help: `
Geometry Package - Geometric Computations

This package provides geometric primitives and computations.
Requires: reals
(Currently in development)

Planned features:
- Points, lines, circles, polygons
- Distances, angles, areas
- Transformations
`
    },

    // Arithmetic functions
    "arith-funs": {
        name: "ArithFuns",
        description: "Polynomials, number theory, rational functions, piecewise functions",
        requires: [],
        functions: [
            // Core Arithmetic
            "Abs", "Sign", "Max", "Min", "Floor", "Ceil", "Round", "Trunc", "Frac",
            "Numer", "Denom", "Reciprocal", "Neg", "Cmp", "Eq", "Lt", "Le", "Gt", "Ge", "Between",
            // Number Theory
            "Gcd", "Lcm", "ExtGcd", "Divides", "Mod", "Quot", "DivMod", "Coprime",
            "IsPrime", "NextPrime", "PrevPrime", "Factor", "Divisors", "DivisorCount", "DivisorSum",
            "ModPow", "ModInv", "EulerPhi", "Mobius",
            "Factorial", "Binomial", "Permutations", "Fibonacci", "Lucas", "Catalan",
            "Harmonic", "Bernoulli",
            // Polynomials
            "Poly", "PolyDeg", "PolyCoeffs", "PolyCoeff", "PolyEval", "PolyHorner",
            "PolyAdd", "PolySub", "PolyMul", "PolyScale", "PolyNeg", "PolyDer", "PolyInt",
            "SynthDiv", "SynthDivPoly", "SynthDivRem", "PolyRebase",
            "PolySignChanges", "PolyDescartes", "PolyRatRoots",
            // Rational Functions
            "RatFunc", "RatFuncNumer", "RatFuncDenom", "RatFuncEval",
            "PartialFrac", "PartialFracSteps",
            // Sparse Polynomials
            "PolySparse", "PolySparseEval",
            // Piecewise
            "Step", "UnitStep", "Rect", "Ramp", "Clamp", "Sgn",
            "Chi", "ChiOpen", "ChiLeftOpen", "ChiRightOpen",
            "Piecewise", "PiecewiseEval"
        ],
        help: `
ArithFuns Package - Arithmetic Functions

Provides polynomials, number theory, rational functions, and piecewise operations.

CATEGORIES:
  Polynomials       - Poly, PolyEval, SynthDiv, PolyRebase, PolyDescartes
  Number Theory     - Gcd, Lcm, Factor, IsPrime, ModPow, EulerPhi
  Rational Funcs    - RatFunc, PartialFrac
  Piecewise         - Piecewise, Step, Rect, Clamp
  Core Arithmetic   - Abs, Sign, Floor, Ceil, Max, Min

POLYNOMIALS:
  Poly({1, 2, 3})           - Create 1 + 2x + 3x²
  PolyEval(P, x)            - Evaluate at x
  SynthDiv(P, c)            - Synthetic division by (x - c)
  PolyRebase(P, a)          - Taylor expansion at x = a
  PolyDescartes(P)          - Descartes' Rule of Signs
  PolyRatRoots(P)           - Find rational roots

NUMBER THEORY:
  Gcd(a, b, ...)            - Greatest common divisor
  Factor(n)                 - Prime factorization
  IsPrime(n)                - Primality test
  ModPow(b, e, m)           - b^e mod m
  Binomial(n, k)            - n choose k

STEP/PIECEWISE:
  Step(x)                   - Heaviside step
  Rect(x, a, b)             - Rectangle function
  Clamp(x, lo, hi)          - Clamp to range

HELP TOPICS:
  HELP polynomial           - Polynomial operations
  HELP synth-div            - Synthetic division details
  HELP number-theory        - Number-theoretic functions
  HELP piecewise            - Piecewise and step functions
`
    },

    // Calculus package (stub)
    calculus: {
        name: "Calculus",
        description: "Symbolic and numeric calculus operations",
        requires: ["reals"],
        functions: [],
        help: `
Calculus Package - Calculus Operations

This package provides calculus operations.
Requires: reals
(Currently in development)

Planned features:
- Numeric differentiation
- Numeric integration
- Series expansions
`
    },

    // ===== BUNDLES =====

    scientific: {
        name: "Scientific",
        description: "Bundle: reals + stats + units - for scientific computing",
        requires: ["reals", "stats", "units"],
        isBundle: true,
        functions: [],
        help: `
Scientific Bundle

Loads a collection of packages useful for scientific computing:
- reals: Transcendental functions (sin, cos, exp, log, etc.)
- stats: Statistical functions
- units: Physical unit conversions

Usage: LOAD scientific
`
    },

    all: {
        name: "All",
        description: "Bundle: Load all available packages",
        requires: ["reals", "units", "stats", "geometry", "arith-funs", "calculus"],
        isBundle: true,
        functions: [],
        help: `
All Bundle

Loads all available packages. Use with caution as some packages
may be in development.

Usage: LOAD all
`
    }
};

/**
 * Get list of all available package names (excluding bundles)
 */
export function getPackageNames() {
    return Object.keys(PackageRegistry).filter(k => !PackageRegistry[k].isBundle);
}

/**
 * Get list of bundle names
 */
export function getBundleNames() {
    return Object.keys(PackageRegistry).filter(k => PackageRegistry[k].isBundle);
}

/**
 * Get package info by name (case-insensitive)
 */
export function getPackageInfo(name) {
    const lower = name.toLowerCase();
    if (PackageRegistry[lower]) {
        return { key: lower, ...PackageRegistry[lower] };
    }
    // Try to find by display name
    for (const [key, pkg] of Object.entries(PackageRegistry)) {
        if (pkg.name.toLowerCase() === lower) {
            return { key, ...pkg };
        }
    }
    return null;
}

/**
 * Resolve package dependencies (returns ordered list of packages to load)
 * @param {string[]} packageNames - Package names to load
 * @param {Set<string>} alreadyLoaded - Set of already loaded package names
 * @returns {string[]} - Ordered list of packages to load
 */
export function resolveDependencies(packageNames, alreadyLoaded = new Set()) {
    const toLoad = [];
    const visited = new Set();

    function visit(name) {
        const lower = name.toLowerCase();
        if (visited.has(lower) || alreadyLoaded.has(lower)) return;
        visited.add(lower);

        const pkg = PackageRegistry[lower];
        if (!pkg) return; // Unknown package

        // Visit dependencies first
        for (const dep of pkg.requires || []) {
            visit(dep);
        }

        // Add this package if not a bundle
        if (!pkg.isBundle) {
            toLoad.push(lower);
        }
    }

    for (const name of packageNames) {
        visit(name);
    }

    return toLoad;
}

/**
 * Generate help text for packages list
 */
export function getPackagesHelpText() {
    let text = "Available Packages:\n\n";

    text += "PACKAGES:\n";
    for (const name of getPackageNames()) {
        const pkg = PackageRegistry[name];
        text += `  ${name.padEnd(12)} - ${pkg.description}\n`;
    }

    text += "\nBUNDLES:\n";
    for (const name of getBundleNames()) {
        const pkg = PackageRegistry[name];
        text += `  ${name.padEnd(12)} - ${pkg.description}\n`;
    }

    text += "\nUsage:\n";
    text += "  LOAD <package>           - Load a single package\n";
    text += "  LOAD <pkg1> <pkg2> ...   - Load multiple packages\n";
    text += "  LOAD <bundle>            - Load a package bundle\n";
    text += "  HELP <package>           - Show package details\n";

    return text;
}
