
import { describe, test, expect, beforeEach } from "bun:test";
import { VariableManager } from "../src/var.js";
import { BaseSystem } from "@ratmath/core";

describe("VariableManager Advanced Scope & Strictness", () => {
    let vm;

    beforeEach(() => {
        // Ensure critical prefixes are registered (fix for test interference)
        if (!BaseSystem.getSystemForPrefix("d")) {
            BaseSystem.registerPrefix("d", BaseSystem.DECIMAL);
        }
        if (!BaseSystem.getSystemForPrefix("x")) {
            BaseSystem.registerPrefix("x", BaseSystem.HEXADECIMAL);
        }

        vm = new VariableManager();
    });

    test("1. Static Variable Scope (Freezing)", () => {
        // a = 10; F(x) -> a + x; a = 20; F(1) should be 11, not 21
        vm.processInput("a = 10");
        vm.processInput("F(x) -> a + x");
        vm.processInput("a = 20");
        const res = vm.processInput("F(1)");
        expect(res.result.toString()).toBe("11");
    });

    test("2. Dynamic Variable Scope (_underscore)", () => {
        // _a = 10; F(x) -> _a + x; _a = 20; F(1) should be 21
        vm.processInput("_a = 10");
        vm.processInput("F(x) -> _a + x");
        vm.processInput("_a = 20");
        const res = vm.processInput("F(1)");
        expect(res.result.toString()).toBe("21");
    });

    test("3. Static Function Capturing (Snapshots)", () => {
        // G(x) -> 2*x; H(y) -> G(y); G(x) -> x+1; H(3) should be 6
        vm.processInput("G(x) -> 2*x");
        vm.processInput("H(y) -> G(y)");
        vm.processInput("G(x) -> x+1"); // Redefine G
        const res = vm.processInput("H(3)");
        expect(res.result.toString()).toBe("6");
    });

    test("4. Base Safety in Function Bodies", () => {
        // F(x) -> x * 10 (defined in DEC)
        // Switch to HEX. F(2) should be 2 * 10(dec) = 20(dec) = 14(hex).
        // If 10 was interpreted as hex 0x10 (16), result would be 32(dec) = 20(hex).
        vm.setInputBase(BaseSystem.DECIMAL);
        vm.processInput("F(x) -> x * 10");

        vm.setInputBase(BaseSystem.HEXADECIMAL);
        const res = vm.processInput("F(2)");
        // 2(hex) * 10(dec) = 20(dec)
        // res.result is an Integer, toString defaults to decimal.
        expect(res.result.toString()).toBe("20");
    });

    test("5. Parameter Ambiguity Check", () => {
        // F(a) in HEX should fail because 'a' is 10
        vm.setInputBase(BaseSystem.HEXADECIMAL);
        const res = vm.processInput("Bad(a) -> a*2");
        expect(res.type).toBe("error");
        expect(res.message).toContain("Ambiguous parameter 'a'");
    });

    test("6. Parameter Auto-Prefixing", () => {
        // F(a) defined in DEC should work in HEX
        vm.setInputBase(BaseSystem.DECIMAL);
        vm.processInput("Good(a) -> a + 10"); // Body: @a + 0d10

        vm.setInputBase(BaseSystem.HEXADECIMAL);
        const res = vm.processInput("Good(2)");
        // 2(param) + 10(dec) = 12(dec)
        expect(res.result.toString()).toBe("12");
    });

    test("7. Static Default Parameters", () => {
        // a = 10; F(x?a) -> x; a = 20; F() should be 10
        vm.processInput("a = 10");
        vm.processInput("F(x?a) -> x");
        vm.processInput("a = 20");
        const res = vm.processInput("F()");
        expect(res.result.toString()).toBe("10");
    });

    test("8. Dynamic Default Parameters", () => {
        // _b = 10; G(x?_b) -> x; _b = 20; G() should be 20
        vm.processInput("_b = 10");
        vm.processInput("G(x?_b) -> x");
        vm.processInput("_b = 20");
        const res = vm.processInput("G()");
        expect(res.result.toString()).toBe("20");
    });

    test("9. Strict Definition Check (Static Variable)", () => {
        // F(x) -> x + y  (where y is undefined) -> Error
        const res = vm.processInput("F(x) -> x + y");
        expect(res.type).toBe("error");
        expect(res.message).toContain("Undefined variable or function 'y' at definition time");
    });

    test("10. Strict Definition Check (Dynamic Variable)", () => {
        // F(x) -> x + _y (where _y is undefined) -> OK
        const res = vm.processInput("F(x) -> x + _y");
        expect(res.type).toBe("function");
    });

    test("11. HOC with Snapshot", () => {
        // H(x) -> x*x
        // Apply(f, v) -> f(v)
        // Test(v) -> Apply(H, v)  <-- H should be snapshotted
        // H(x) -> x+1
        // Test(4) -> Should use x*x (16), not x+1 (5)
        vm.processInput("H(x) -> x*x");
        vm.processInput("Apply(f, v) -> f(v)");
        vm.processInput("Test(v) -> Apply(H, v)");

        vm.processInput("H(x) -> x+1");
        const res = vm.processInput("Test(4)");
        expect(res.result.toString()).toBe("16");
    });

});
