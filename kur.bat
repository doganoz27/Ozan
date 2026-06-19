@echo off
chcp 65001 >nul
echo.
echo  ████████╗██╗████████╗ █████╗ ███╗   ██╗    ███████╗██╗      ██████╗ ██╗    ██╗
echo  ╚══██╔══╝██║╚══██╔══╝██╔══██╗████╗  ██║    ██╔════╝██║     ██╔═══██╗██║    ██║
echo     ██║   ██║   ██║   ███████║██╔██╗ ██║    █████╗  ██║     ██║   ██║██║ █╗ ██║
echo     ██║   ██║   ██║   ██╔══██║██║╚██╗██║    ██╔══╝  ██║     ██║   ██║██║███╗██║
echo     ██║   ██║   ██║   ██║  ██║██║ ╚████║    ██║     ███████╗╚██████╔╝╚███╔███╔╝
echo     ╚═╝   ╚═╝   ╚═╝   ╚═╝  ╚═╝╚═╝  ╚═══╝    ╚═╝     ╚══════╝ ╚═════╝  ╚══╝╚══╝
echo.
echo  KURULUM BASLIYOR...
echo  ================================================
echo.

REM --- Python kontrolu ---
python --version >nul 2>&1
if errorlevel 1 (
    echo  [HATA] Python bulunamadi!
    echo  https://python.org adresinden Python yukle.
    pause
    exit /b 1
)
echo  [OK] Python bulundu.

REM --- Kutuphaneleri yukle ---
echo.
echo  [1/2] Kutuphaneler yukleniyor...
pip install rich numpy requests websocket-client yfinance --quiet --upgrade
if errorlevel 1 (
    echo  [HATA] Kutuphaneler yuklenemedi!
    pause
    exit /b 1
)
echo  [OK] Kutuphaneler hazir.

REM --- titan_flow.py indir ---
echo.
echo  [2/2] titan_flow.py indiriliyor...
curl -L -o "%~dp0titan_flow.py" "https://raw.githubusercontent.com/doganoz27/Ozan/claude/zen-albattani-Jvp6h/titan_flow.py" --ssl-no-revoke
if errorlevel 1 (
    echo  [HATA] Dosya indirilemedi! Internet baglantisini kontrol et.
    pause
    exit /b 1
)
echo  [OK] titan_flow.py indirildi.

echo.
echo  ================================================
echo  KURULUM TAMAMLANDI — Terminal baslatiliyor...
echo  ================================================
echo.
echo  Cikis icin: Ctrl+C
echo.
timeout /t 2 >nul

REM --- Calistir ---
python "%~dp0titan_flow.py"

echo.
echo  Terminal kapandi.
pause
