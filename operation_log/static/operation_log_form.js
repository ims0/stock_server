const toolbar = document.querySelector('.editor-toolbar');

if (toolbar) {
  const uploadUrl = toolbar.dataset.uploadUrl;
  const previewUrl = toolbar.dataset.previewUrl;
  const uploadButton = document.getElementById('upload-image-button');
  const uploadInput = document.getElementById('image-upload-input');
  const altInput = document.getElementById('image-alt-text');
  const statusNode = document.getElementById('upload-status');
  const contentField = document.querySelector('textarea[name="content"]');
  const coverField = document.querySelector('input[name="cover_image_url"]');
  const titleField = document.querySelector('input[name="title"]');
  const previewTitle = document.getElementById('preview-title');
  const previewBody = document.getElementById('preview-body');
  const previewCover = document.getElementById('preview-cover');
  const previewCoverImage = previewCover?.querySelector('img');
  const previewStatus = document.getElementById('preview-status');
  let previewTimer = null;

  const setStatus = (message, kind = '') => {
    statusNode.textContent = message;
    statusNode.className = `toolbar-status${kind ? ` ${kind}` : ''}`;
  };

  const insertAtCursor = (field, text) => {
    const start = field.selectionStart ?? field.value.length;
    const end = field.selectionEnd ?? field.value.length;
    const prefix = field.value.slice(0, start);
    const suffix = field.value.slice(end);
    const spacerBefore = prefix && !prefix.endsWith('\n') ? '\n\n' : '';
    const spacerAfter = suffix && !suffix.startsWith('\n') ? '\n\n' : '\n';
    field.value = `${prefix}${spacerBefore}${text}${spacerAfter}${suffix}`;
    const nextPosition = (prefix + spacerBefore + text + spacerAfter).length;
    field.focus();
    field.setSelectionRange(nextPosition, nextPosition);
  };

  const setPreviewStatus = (message, kind = '') => {
    if (!previewStatus) {
      return;
    }
    previewStatus.textContent = message;
    previewStatus.className = `preview-status${kind ? ` ${kind}` : ''}`;
  };

  const syncPreviewMeta = () => {
    if (previewTitle && titleField) {
      previewTitle.textContent = titleField.value.trim() || '技术文档标题预览';
    }
    if (previewCover && previewCoverImage && coverField) {
      const coverUrl = coverField.value.trim();
      previewCover.classList.toggle('is-hidden', !coverUrl);
      if (coverUrl) {
        previewCoverImage.src = coverUrl;
      } else {
        previewCoverImage.removeAttribute('src');
      }
    }
  };

  previewCoverImage?.addEventListener('error', () => {
    if (previewCover) {
      previewCover.classList.add('is-hidden');
    }
    if (coverField) {
      coverField.value = '';
    }
    if (previewCoverImage) {
      previewCoverImage.removeAttribute('src');
    }
    setPreviewStatus('封面图加载失败，已自动清空封面设置。', 'error');
  });

  const refreshPreview = async () => {
    if (!previewUrl || !contentField || !previewBody) {
      return;
    }

    syncPreviewMeta();
    setPreviewStatus('预览更新中...', 'loading');

    const formData = new FormData();
    formData.append('content', contentField.value);

    try {
      const response = await fetch(previewUrl, {
        method: 'POST',
        body: formData,
        credentials: 'same-origin',
      });
      const payload = await response.json();
      if (!response.ok) {
        throw new Error(payload.error || '预览生成失败');
      }
      previewBody.innerHTML = payload.html || '<p class="empty">暂无正文内容</p>';
      setPreviewStatus('预览已同步', 'success');
    } catch (error) {
      setPreviewStatus(error instanceof Error ? error.message : '预览生成失败', 'error');
    }
  };

  const schedulePreview = () => {
    if (previewTimer) {
      window.clearTimeout(previewTimer);
    }
    previewTimer = window.setTimeout(() => {
      refreshPreview();
    }, 220);
  };

  uploadButton?.addEventListener('click', async () => {
    const file = uploadInput?.files?.[0];
    if (!file) {
      setStatus('请选择一张图片后再上传。', 'error');
      return;
    }

    const formData = new FormData();
    formData.append('image', file);
    formData.append('alt_text', altInput?.value?.trim() || '');

    uploadButton.disabled = true;
    setStatus('图片上传中...');

    try {
      const response = await fetch(uploadUrl, {
        method: 'POST',
        body: formData,
        credentials: 'same-origin',
      });
      const payload = await response.json();
      if (!response.ok) {
        throw new Error(payload.error || '上传失败');
      }

      insertAtCursor(contentField, payload.markdown);
      if (coverField && !coverField.value) {
        coverField.value = payload.url;
      }
      syncPreviewMeta();
      schedulePreview();
      uploadInput.value = '';
      if (altInput) {
        altInput.value = '';
      }
      setStatus('上传成功，已插入 Markdown。', 'success');
    } catch (error) {
      setStatus(error instanceof Error ? error.message : '上传失败', 'error');
    } finally {
      uploadButton.disabled = false;
    }
  });

  titleField?.addEventListener('input', syncPreviewMeta);
  coverField?.addEventListener('input', syncPreviewMeta);
  contentField?.addEventListener('input', schedulePreview);

  syncPreviewMeta();
  refreshPreview();
}