import { describe, test, expect, beforeEach } from "bun:test";
import { VariableManager } from "../src/var.js";

describe("Piecewise {{ }} Syntax", () => {
    let vm;

    beforeEach(() => {
        vm = new VariableManager();
    });

    describe("Basic Piecewise", () => {
        test("simple condition ? value with two cases", () => {
            expect(vm.processInput("{{1 ? 5, 7}}").result.toString()).toBe("5");
            expect(vm.processInput("{{0 ? 5, 7}}").result.toString()).toBe("7");
        });

        test("comparison operators in condition", () => {
            expect(vm.processInput("{{2>1 ? 5, 7}}").result.toString()).toBe("5");
            expect(vm.processInput("{{2>3 ? 5, 7}}").result.toString()).toBe("7");
            expect(vm.processInput("{{2<3 ? 5, 7}}").result.toString()).toBe("5");
            expect(vm.processInput("{{2<1 ? 5, 7}}").result.toString()).toBe("7");
        });

        test(">= and <= operators work correctly", () => {
            expect(vm.processInput("{{2>=2 ? 5, 7}}").result.toString()).toBe("5");
            expect(vm.processInput("{{2<=2 ? 5, 7}}").result.toString()).toBe("5");
            expect(vm.processInput("{{3>=5 ? 5, 7}}").result.toString()).toBe("7");
            expect(vm.processInput("{{3<=1 ? 5, 7}}").result.toString()).toBe("7");
        });

        test(">= and <= with spaces", () => {
            expect(vm.processInput("{{2 >= 2 ? 5, 7}}").result.toString()).toBe("5");
            expect(vm.processInput("{{2 <= 2 ? 5, 7}}").result.toString()).toBe("5");
        });

        test("== and != operators", () => {
            expect(vm.processInput("{{2==2 ? 5, 7}}").result.toString()).toBe("5");
            expect(vm.processInput("{{2!=2 ? 5, 7}}").result.toString()).toBe("7");
            expect(vm.processInput("{{2!=3 ? 5, 7}}").result.toString()).toBe("5");
        });
    });

    describe("Default Value", () => {
        test("last value without condition is default", () => {
            expect(vm.processInput("{{0 ? 5, 100}}").result.toString()).toBe("100");
        });

        test("multiple conditions with default", () => {
            expect(vm.processInput("{{0 ? 1, 0 ? 2, 99}}").result.toString()).toBe("99");
        });

        test("first matching condition wins", () => {
            expect(vm.processInput("{{1 ? 10, 1 ? 20, 99}}").result.toString()).toBe("10");
        });

        test("sign function pattern", () => {
            vm.processInput("x = 5");
            expect(vm.processInput("{{x>0 ? 1, x<0 ? -1, 0}}").result.toString()).toBe("1");
            
            vm.processInput("x = -5");
            expect(vm.processInput("{{x>0 ? 1, x<0 ? -1, 0}}").result.toString()).toBe("-1");
            
            vm.processInput("x = 0");
            expect(vm.processInput("{{x>0 ? 1, x<0 ? -1, 0}}").result.toString()).toBe("0");
        });
    });

    describe("Multiple Conditions", () => {
        test("three conditions with values", () => {
            vm.processInput("x = 15");
            expect(vm.processInput("{{x>10 ? 100, x>5 ? 50, x>0 ? 10, 0}}").result.toString()).toBe("100");
            
            vm.processInput("x = 7");
            expect(vm.processInput("{{x>10 ? 100, x>5 ? 50, x>0 ? 10, 0}}").result.toString()).toBe("50");
            
            vm.processInput("x = 3");
            expect(vm.processInput("{{x>10 ? 100, x>5 ? 50, x>0 ? 10, 0}}").result.toString()).toBe("10");
            
            vm.processInput("x = -1");
            expect(vm.processInput("{{x>10 ? 100, x>5 ? 50, x>0 ? 10, 0}}").result.toString()).toBe("0");
        });
    });

    describe("Function Definition with Piecewise", () => {
        test("abs function using piecewise", () => {
            vm.processInput("Abs2 = x -> {{x>=0 ? x, -x}}");
            expect(vm.processInput("Abs2(5)").result.toString()).toBe("5");
            expect(vm.processInput("Abs2(-5)").result.toString()).toBe("5");
            expect(vm.processInput("Abs2(0)").result.toString()).toBe("0");
        });

        test("step function using piecewise", () => {
            vm.processInput("Step2 = x -> {{x>=0 ? 1, 0}}");
            expect(vm.processInput("Step2(5)").result.toString()).toBe("1");
            expect(vm.processInput("Step2(-5)").result.toString()).toBe("0");
            expect(vm.processInput("Step2(0)").result.toString()).toBe("1");
        });

        test("sign function using piecewise", () => {
            vm.processInput("Sgn2 = x -> {{x>0 ? 1, x<0 ? -1, 0}}");
            expect(vm.processInput("Sgn2(5)").result.toString()).toBe("1");
            expect(vm.processInput("Sgn2(-5)").result.toString()).toBe("-1");
            expect(vm.processInput("Sgn2(0)").result.toString()).toBe("0");
        });

        test("clamp function using piecewise", () => {
            vm.processInput("lo = 0");
            vm.processInput("hi = 10");
            vm.processInput("Clamp2 = x -> {{x<lo ? lo, x>hi ? hi, x}}");
            expect(vm.processInput("Clamp2(5)").result.toString()).toBe("5");
            expect(vm.processInput("Clamp2(-5)").result.toString()).toBe("0");
            expect(vm.processInput("Clamp2(15)").result.toString()).toBe("10");
        });
    });

    describe("Error Handling", () => {
        test("no matching condition and no default throws error", () => {
            const result = vm.processInput("{{0 ? 5, 0 ? 10}}");
            expect(result.type).toBe("error");
            expect(result.message).toContain("no matching condition");
        });

        test("unconditional value not at end throws error", () => {
            const result = vm.processInput("{{5, 0 ? 10}}");
            expect(result.type).toBe("error");
            expect(result.message).toContain("default value must be last");
        });
    });

    describe("Complex Expressions", () => {
        test("arithmetic in condition", () => {
            expect(vm.processInput("{{2+3>4 ? 100, 0}}").result.toString()).toBe("100");
            expect(vm.processInput("{{2*3>=6 ? 100, 0}}").result.toString()).toBe("100");
        });

        test("arithmetic in value", () => {
            expect(vm.processInput("{{1 ? 2+3, 0}}").result.toString()).toBe("5");
            expect(vm.processInput("{{1 ? 2*3, 0}}").result.toString()).toBe("6");
        });

        test("nested piecewise", () => {
            vm.processInput("x = 5");
            // Outer: if x>0, evaluate inner piecewise
            expect(vm.processInput("{{x>0 ? {{x>10 ? 100, 50}}, 0}}").result.toString()).toBe("50");
            
            vm.processInput("x = 15");
            expect(vm.processInput("{{x>0 ? {{x>10 ? 100, 50}}, 0}}").result.toString()).toBe("100");
        });
    });
});
