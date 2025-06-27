// src/utils/BoyerMooreSearcher.js

class BoyerMooreSearcher {
    /**
     * Initializes the BoyerMooreSearcher with a given pattern.
     * @param {string} pattern The pattern to search for.
     */
    constructor(pattern) {
      this.pattern = pattern;
      this.badCharTable = this.#buildBadCharTable(pattern); // Private helper method
    }
  
    /**
     * Builds the bad character heuristic table for the given pattern.
     * This is a private helper method.
     * @param {string} pattern The pattern string.
     * @returns {number[]} An array representing the bad character table.
     */
    #buildBadCharTable(pattern) {
      const table = new Array(256).fill(-1);
      for (let i = 0; i < pattern.length; i++) {
        // Store the rightmost occurrence of each character in the pattern
        table[pattern.charCodeAt(i)] = i;
      }
      return table;
    }
  
    /**
     * Performs the Boyer-Moore search algorithm on the given text.
     * @param {string} text The text to search within.
     * @returns {number[]} An array of starting indices where the pattern is found.
     */
    search(text) {
      const matches = [];
      const m = this.pattern.length;
      const n = text.length;
  
      if (m === 0) {
        for (let i = 0; i <= n; i++) {
          matches.push(i);
        }
        return matches;
      }
      if (n === 0 || m > n) {
        return matches;
      }
  
      let shift = 0;
  
      while (shift <= n - m) {
        let j = m - 1;
  
        while (j >= 0 && this.pattern[j].toLowerCase() === text[shift + j].toLowerCase()) {
          j--;
        }
  
        if (j < 0) {
          matches.push(shift);
          // Adjusted shift logic for after a match
          const nextCharInTextCode = (shift + m < n) ? text.charCodeAt(shift + m) : -1;
          const badCharVal = (nextCharInTextCode !== -1) ? this.badCharTable[nextCharInTextCode] : -1;
          shift += (shift + m < n) ? (m - (badCharVal !== -1 ? badCharVal : -1)) : 1;
          if (shift + m >= n && (this.badCharTable[text.charCodeAt(shift + m -1)] === -1 || m - this.badCharTable[text.charCodeAt(shift + m -1)] === 0)) {
              shift += 1;
          }
  
        } else {
          const badCharShift = this.badCharTable[text.charCodeAt(shift + j)];
          shift += Math.max(1, j - badCharShift);
        }
      }
  
      return matches;
    }
  }
  
  // Export the class so it can be imported in other files
  export default BoyerMooreSearcher;