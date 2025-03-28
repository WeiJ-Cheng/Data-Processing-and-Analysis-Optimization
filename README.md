# Data-Processing-and-Analysis-Optimization

## 專案名稱
**Walmart Repricing and Settlement Analysis**

## 程式介紹
這段程式碼是一個針對 Walmart 平台的商品價格重定價及結算費用分析的工具。其主要目的是通過處理和分析來自不同來源的報表數據，生成商品的最新定價、成本結算及其他相關指標，從而幫助管理層進行價格調整、成本控制和銷售優化。程式將來自多個資料來源的報告（如 Looker、Settlement 和 Order Report 等）整合並進行清理、處理，最終輸出優化後的商品信息表格。

## 功能
1. **資料導入與清理**：從 Google Drive 及各類 CSV 和 Excel 檔案導入資料，並處理缺失值、重複數據等問題。
2. **商品成本計算與合併**：通過合併 Looker 資料和 Admin 資料來計算商品的平均成本，並處理來自不同供應商的價格數據。
3. **運費及結算費用計算**：計算並合併運費、備貨費用等資料，將其與商品資訊結合，並處理各類費用的計算（例如，WFS 服務費和 Prep 服務費）。
4. **商品銷售與庫存分析**：根據最近的訂單資料和庫存資料計算商品的銷售狀況、庫存情況及補貨需求。
5. **報表與數據匯出**：根據處理結果生成一個包含商品詳情、銷售狀況、定價等資料的 Excel 報表，並匯出至 Google Drive。

## 使用技術
- **Python**：主程式語言，用於數據處理、清理和分析。
- **Pandas**：用於數據處理和分析，特別是資料的合併、過濾和聚合。
- **NumPy**：用於數值計算和資料處理。
- **Matplotlib**：雖然在這段程式中沒有大量的視覺化需求，但這是用來生成圖表的庫。
- **Pytz**：用於處理不同時區的時間。
- **Google Colab**：程式運行環境，並且與 Google Drive 進行數據存取和保存。
- **Excel (openpyxl)**：用於將最終處理的數據匯出為 Excel 檔案。

## 安裝與運行

1. 將此程式放入您的 Python 環境中，並確保已安裝所需的套件：
    ```bash
    pip install pytz pandas matplotlib
    ```

2. 在 Google Colab 中運行此程式時，請掛載 Google Drive，並根據需要更新檔案路徑。

3. 載入所需的 CSV 和 Excel 檔案（例如，Walmart Reprice、Settlement 及其他報告），程式將會處理和合併這些數據，並生成報表。

4. 最終生成的報表會被存儲在您的 Google Drive 中，或匯出為 Excel 檔案。

## 注意事項
1. **檔案替換**：由於一些原因，檔案無法提供，請自行替換程式中的檔案路徑和檔案名稱為您自己的資料檔案。
2. **操作圖片檔**：目前不便提供操作圖片檔，若需要視覺化操作的幫助，請參考程式碼中的詳細注解。
3. **Looker Studio 視覺化**：檔案整理後，您可以使用 Looker Studio 將數據進行進一步的視覺化，並創建自訂的報告與儀表板。

## 貢獻

如有任何建議或錯誤修正，請提出 Pull Request。您的貢獻對我們非常重要！

## 版權資訊

此專案使用 [MIT License](LICENSE) 開源協議。
