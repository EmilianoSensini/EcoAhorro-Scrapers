const fs = require('fs/promises');

const SUCURSALES = "9-1-127,24-1-357,2002-1-36,2005-1-59,13-1-24,13-1-42,9-1-122,11-5-3602,13-1-2,13-1-147,61-1-14,13-1-11,13-1-54,13-1-32,13-1-1,13-1-88,13-1-98,13-1-85,2005-1-44,2-1-215,15-1-1078,2005-1-27,24-1-364,9-1-704,13-1-55,15-1-5608,13-1-73,2005-1-39,15-1-297,2-1-069";

const esperar = (ms) => new Promise(resolve => setTimeout(resolve, ms));

const escaparCSV = (texto) => {
    if (!texto) return '""';
    const cadena = String(texto).replace(/\n/g, ' ').replace(/"/g, '""');
    return `"${cadena}"`;
};

// Array global para acumular los resultados de todas las ramas
const todosLosProductos = [];

// Función para consultar rápidamente cuántos productos tiene una categoría (limit=1)
async function obtenerTotal(idCategoria) {
    const url = `https://d3e6htiiul5ek9.cloudfront.net/prod/productos?id_categoria=${idCategoria}&array_sucursales=${SUCURSALES}&limit=1`;
    try {
        const respuesta = await fetch(url, {
            headers: {
                "accept": "application/json",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
            }
        });
        if (!respuesta.ok) return 0;
        const datos = await respuesta.json();
        return datos.total || 0;
    } catch (error) {
        return 0;
    }
}

// Función que realiza el scraping de productos una vez que encontramos el nivel más profundo
async function extraerProductosDe(idCategoria, totalEstimado) {
    const limite = 50;
    let offset = 0;
    
    console.log(`\n--- Extrayendo ${totalEstimado} productos de la categoría específica: ${idCategoria} ---`);

    while (offset < totalEstimado) {
        const url = `https://d3e6htiiul5ek9.cloudfront.net/prod/productos?id_categoria=${idCategoria}&array_sucursales=${SUCURSALES}&offset=${offset}&limit=${limite}&sort=-cant_sucursales_disponible`;
        try {
            const respuesta = await fetch(url, {
                headers: { "accept": "application/json", "User-Agent": "Mozilla/5.0" }
            });

            if (!respuesta.ok) break;

            const datos = await respuesta.json();
            const productos = datos.productos || [];
            
            if (productos.length === 0) break;

            productos.forEach(p => {
                todosLosProductos.push({
                    id: p.id || '',
                    nombre: p.nombre || '',
                    marca: p.marca || '',
                    presentacion: p.presentacion || '',
                    id_categoria: idCategoria
                });
            });

            console.log(`[Categoría ${idCategoria}] Procesados ${Math.min(offset + productos.length, totalEstimado)} de ${totalEstimado}...`);
            offset += limite;
            await esperar(1500); // Pausa para no saturar el servidor

        } catch (error) {
            console.error(`Error de red en categoría ${idCategoria}:`, error.message);
            break;
        }
    }
}

// Función recursiva que explora los niveles (Padre -> Hijo -> Nieto)
async function explorarNiveles(idActual) {
    // Si la categoría tiene 6 dígitos, ya es "Nieto". Es el tope de profundidad.
    if (idActual.length >= 6) {
        const total = await obtenerTotal(idActual);
        if (total > 0) await extraerProductosDe(idActual, total);
        return;
    }

    // Probamos si existe el primer subnivel (ej: si idActual es '02', probamos '0201')
    const primerSubId = idActual + '01';
    const totalPrimerSub = await obtenerTotal(primerSubId);

    if (totalPrimerSub > 0) {
        // Tiene subcategorías. Iteramos para recorrerlas todas ('01', '02', '03'...)
        let i = 1;
        let vaciosConsecutivos = 0;
        
        // Toleramos hasta 2 vacíos consecutivos por si la numeración pega un salto (ej: pasa de 0102 a 0104)
        while (vaciosConsecutivos < 2) {
            const subId = idActual + i.toString().padStart(2, '0');
            const totalSub = await obtenerTotal(subId);

            if (totalSub > 0) {
                vaciosConsecutivos = 0;
                // Llamada recursiva para que evalúe si este "Hijo" tiene "Nietos" o no
                await explorarNiveles(subId); 
            } else {
                vaciosConsecutivos++;
            }
            i++;
        }
    } else {
        // No tiene subcategorías. Este es el nivel más bajo posible. Extraemos aquí.
        const totalActual = await obtenerTotal(idActual);
        if (totalActual > 0) {
            await extraerProductosDe(idActual, totalActual);
        }
    }
}

async function iniciarExtraccion() {
    console.log("Iniciando exploración profunda de categorías...");
    
    // Exploramos las categorías raíz (Padres) del 01 al 99
    for (let i = 11; i <=100; i++) {
        const idPadre = i.toString().padStart(2, '0');
        const totalPadre = await obtenerTotal(idPadre);
        
        // Solo iniciamos la rama si la categoría principal tiene productos
        if (totalPadre > 0) {
            console.log(`\nAnalizando árbol de categoría raíz: ${idPadre}`);
            await explorarNiveles(idPadre);
        }
    }

    console.log("\nExtracción completa en todas las ramas. Generando archivo CSV...");
    await generarArchivoCSV(todosLosProductos);
}

async function generarArchivoCSV(productos) {
    const encabezados = ["ID/EAN", "NOMBRE", "MARCA", "PRESENTACION", "ID_CATEGORIA"];
    
    const filas = productos.map(p => [
        escaparCSV(p.id),
        escaparCSV(p.nombre),
        escaparCSV(p.marca),
        escaparCSV(p.presentacion),
        escaparCSV(p.id_categoria)
    ].join(","));

    const contenidoCompleto = [encabezados.join(","), ...filas].join("\n");

    try {
        await fs.writeFile('catalogo_productos.csv', contenidoCompleto, 'utf-8');
        console.log(`Archivo 'catalogo_productos.csv' guardado exitosamente con ${productos.length} productos únicos en las categorías más bajas posibles.`);
    } catch (error) {
        console.error("Error al guardar el archivo:", error);
    }
}

iniciarExtraccion();