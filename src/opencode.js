import fs from "fs";
import "dotenv/config";

// Verificação básica
if (!process.env.OPENAI_API_KEY) {
  console.error("❌ OPENAI_API_KEY não encontrada");
  process.exit(1);
}

console.log("✅ OPENAI_API_KEY carregada");

// Conteúdo gerado (exemplo simples)
const content = `
// Arquivo gerado automaticamente pelo OpenCode
// Data: ${new Date().toISOString()}

export function helloOpenCode() {
  return "OpenCode está funcionando 🚀";
}
`;

// Garante que a pasta existe
fs.mkdirSync("src/generated", { recursive: true });

// Cria arquivo gerado
fs.writeFileSync("src/generated/hello.js", content);

console.log("✅ Arquivo src/generated/hello.js criado com sucesso");
