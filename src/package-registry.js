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
        description: "Oracle-based computation for real number operations",
        requires: [],
        functions: [],
        help: `
Oracles Package - Oracle-Based Computation

This package provides oracle-based computation methods for
exact real arithmetic operations.
(Currently in development)
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
        description: "Additional arithmetic functions (GCD, LCM, primes, etc.)",
        requires: [],
        functions: [],
        help: `
ArithFuns Package - Arithmetic Functions

This package provides additional number-theoretic functions.
(Currently in development)

Planned features:
- GCD, LCM, extended Euclidean algorithm
- Prime testing, factorization
- Modular arithmetic
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
