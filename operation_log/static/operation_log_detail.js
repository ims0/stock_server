document.querySelectorAll('.article-cover img').forEach((imageNode) => {
  imageNode.addEventListener('error', () => {
    const container = imageNode.closest('.article-cover');
    if (container) {
      container.classList.add('is-hidden');
    }
  });
});